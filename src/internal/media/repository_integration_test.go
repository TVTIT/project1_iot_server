package media

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/config"
	"iot-platform/internal/mqttcredential"
)

func requireMediaPostgres(t *testing.T) (*pgxpool.Pool, *pgxpool.Pool) {
	t.Helper()
	if os.Getenv("MEDIA_TEST_ISOLATED") != "1" {
		t.Skip("skipping native media integration: require MEDIA_TEST_ISOLATED=1 owned fixture")
	}
	setupDSN, backendDSN := os.Getenv("MEDIA_TEST_SETUP_URL"), os.Getenv("MEDIA_TEST_BACKEND_URL")
	if setupDSN == "" || backendDSN == "" {
		t.Fatal("MEDIA_TEST_SETUP_URL and MEDIA_TEST_BACKEND_URL are required for isolated media tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	setup, err := pgxpool.New(ctx, setupDSN)
	if err != nil {
		t.Fatalf("connect isolated media setup DB: %v", err)
	}
	backend, err := pgxpool.New(ctx, backendDSN)
	if err != nil {
		setup.Close()
		t.Fatalf("connect isolated media backend DB: %v", err)
	}
	t.Cleanup(func() { backend.Close(); setup.Close() })
	var version int
	var name string
	if err := setup.QueryRow(ctx, `SELECT version,name FROM public.schema_migrations ORDER BY version DESC LIMIT 1`).Scan(&version, &name); err != nil || version != 28 || name != "stage8_media_metadata" {
		t.Fatalf("media tests require exact migration 28 terminal: version=%d name=%q err=%v", version, name, err)
	}
	return setup, backend
}

func mediaLimits() config.MediaUploadConfig {
	return config.MediaUploadConfig{
		MaxUploadBytes: MaxUploadBytes, MaxPendingPerGateway: 1, MaxUnsettledIntents: 2,
		TemporaryBytes: 2 * MaxUploadBytes, StoredBytes: 4 * MaxUploadBytes,
		IntentTTL: time.Minute,
	}
}

func mediaGateway(t *testing.T, setup *pgxpool.Pool, status string) string {
	t.Helper()
	id := "media_" + uuid.NewString()[:12]
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := setup.Exec(ctx, `INSERT INTO public.gateways(gateway_id,name) VALUES($1,'media test')`, id)
	if err != nil {
		t.Fatal(err)
	}
	if status == "active" {
		seedMediaCredential(t, setup, id, false)
	}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_, _ = setup.Exec(cleanup, `DELETE FROM public.media_objects WHERE gateway_id=$1`, id)
		_, _ = setup.Exec(cleanup, `DELETE FROM public.mqtt_credential_recovery WHERE gateway_id=$1`, id)
		_, _ = setup.Exec(cleanup, `DELETE FROM public.mqtt_credential_maintenance WHERE gateway_id=$1`, id)
		_, _ = setup.Exec(cleanup, `DELETE FROM public.mqtt_rotation_replacements WHERE gateway_id=$1`, id)
		_, _ = setup.Exec(cleanup, `DELETE FROM public.gateway_mqtt_credentials WHERE gateway_id=$1`, id)
		_, _ = setup.Exec(cleanup, `DELETE FROM public.gateway_mqtt_credential_events WHERE gateway_id=$1`, id)
		_, _ = setup.Exec(cleanup, `DELETE FROM public.gateways WHERE gateway_id=$1`, id)
		_, _ = setup.Exec(cleanup, `DELETE FROM public.platform_admins WHERE user_id IN (SELECT id FROM public.profiles WHERE full_name='media fixture admin')`)
		_, _ = setup.Exec(cleanup, `DELETE FROM auth.users WHERE id IN (SELECT id FROM public.profiles WHERE full_name='media fixture admin')`)
		_, _ = setup.Exec(cleanup, `DELETE FROM public.profiles WHERE full_name='media fixture admin'`)
	})
	return id
}

func seedMediaCredential(t *testing.T, setup *pgxpool.Pool, gatewayID string, revoke bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	actor := uuid.New()
	if _, err := setup.Exec(ctx, `INSERT INTO auth.users(id,email) VALUES($1,$2)`, actor, actor.String()+"@fixture.invalid"); err != nil {
		t.Fatal(err)
	}
	if _, err := setup.Exec(ctx, `UPDATE public.profiles SET full_name='media fixture admin' WHERE id=$1`, actor); err != nil {
		t.Fatal(err)
	}
	if _, err := setup.Exec(ctx, `INSERT INTO public.platform_admins(user_id) VALUES($1)`, actor); err != nil {
		t.Fatal(err)
	}
	repo, err := mqttcredential.NewPostgresRepository(setup, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	provisionID := uuid.New()
	provision, err := repo.BeginSimplifiedOperation(ctx, mqttcredential.BeginRequest{
		ActorUserID: actor, OperationID: provisionID,
		Input: mqttcredential.MutationInput{GatewayID: gatewayID, IdempotencyKey: uuid.New(), Action: mqttcredential.ActionProvision},
	})
	if err != nil {
		t.Fatalf("fixture credential provision admission: %v", err)
	}
	evidence := mqttcredential.VerificationEvidence{RAMApplied: true, FreshPositiveVerified: true}
	completion, err := mqttcredential.CompleteOperation(provision.Operation, mqttcredential.ExecutionVerifiedSuccess, evidence, time.Now().UTC().Round(time.Microsecond))
	if err != nil {
		t.Fatal(err)
	}
	_, err = repo.ConditionalFinalize(ctx, mqttcredential.OperationUpdate{
		Guard: mqttcredential.OperationGuard{GatewayID: gatewayID, ExpectedOperationID: provisionID,
			ExpectedOperationStatus: mqttcredential.OperationPending, ExpectedCredentialStatus: mqttcredential.CredentialProvisioning,
			ExpectedCredentialVersion: 1},
		Completion: completion,
	})
	if err != nil {
		t.Fatalf("fixture credential provision finalization: %v", err)
	}
	if revoke {
		revokeID := uuid.New()
		begun, err := repo.BeginSimplifiedOperation(ctx, mqttcredential.BeginRequest{
			ActorUserID: actor, OperationID: revokeID,
			Input: mqttcredential.MutationInput{GatewayID: gatewayID, IdempotencyKey: uuid.New(), Action: mqttcredential.ActionRevoke},
		})
		if err != nil {
			t.Fatalf("fixture credential revoke admission: %v", err)
		}
		revokeEvidence := mqttcredential.VerificationEvidence{RAMApplied: true}
		revokeCompletion, err := mqttcredential.CompleteOperation(begun.Operation, mqttcredential.ExecutionVerifiedSuccess, revokeEvidence, time.Now().UTC().Round(time.Microsecond))
		if err != nil {
			t.Fatal(err)
		}
		_, err = repo.ConditionalFinalize(ctx, mqttcredential.OperationUpdate{
			Guard: mqttcredential.OperationGuard{GatewayID: gatewayID, ExpectedOperationID: revokeID,
				ExpectedOperationStatus: mqttcredential.OperationPending, ExpectedCredentialStatus: mqttcredential.CredentialRevoking,
				ExpectedCredentialVersion: 1},
			Completion: revokeCompletion,
		})
		if err != nil {
			t.Fatalf("fixture credential revoke finalization: %v", err)
		}
	}
}

func revokeMediaCredential(t *testing.T, setup *pgxpool.Pool, gatewayID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var actor uuid.UUID
	if err := setup.QueryRow(ctx, `SELECT id FROM public.profiles WHERE full_name='media fixture admin' LIMIT 1`).Scan(&actor); err != nil {
		t.Fatal(err)
	}
	repo, err := mqttcredential.NewPostgresRepository(setup, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	opID := uuid.New()
	begun, err := repo.BeginSimplifiedOperation(ctx, mqttcredential.BeginRequest{
		ActorUserID: actor, OperationID: opID,
		Input: mqttcredential.MutationInput{GatewayID: gatewayID, IdempotencyKey: uuid.New(), Action: mqttcredential.ActionRevoke},
	})
	if err != nil {
		t.Fatal(err)
	}
	completion, err := mqttcredential.CompleteOperation(begun.Operation, mqttcredential.ExecutionVerifiedSuccess,
		mqttcredential.VerificationEvidence{RAMApplied: true}, time.Now().UTC().Round(time.Microsecond))
	if err != nil {
		t.Fatal(err)
	}
	_, err = repo.ConditionalFinalize(ctx, mqttcredential.OperationUpdate{
		Guard: mqttcredential.OperationGuard{GatewayID: gatewayID, ExpectedOperationID: opID,
			ExpectedOperationStatus: mqttcredential.OperationPending, ExpectedCredentialStatus: mqttcredential.CredentialRevoking,
			ExpectedCredentialVersion: 1},
		Completion: completion,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func mediaRequest(gatewayID, upload string) Admission {
	return Admission{GatewayID: gatewayID, UploadID: upload, CapturedAt: time.Now().UTC(),
		ExpectedSize: 100, DeclaredSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
}

func TestPostgresAdmissionReplayConflictAndAuthorization(t *testing.T) {
	setup, backend := requireMediaPostgres(t)
	repo, err := NewRepository(backend, mediaLimits(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	gw := mediaGateway(t, setup, "active")
	ctx := context.Background()
	req := mediaRequest(gw, "same_upload")
	start := make(chan struct{})
	ids := make(chan uuid.UUID, 2)
	errs := make(chan error, 2)
	var concurrent sync.WaitGroup
	for range 2 {
		concurrent.Add(1)
		go func() {
			defer concurrent.Done()
			<-start
			got, admitErr := repo.Admit(ctx, req)
			ids <- got.MediaID
			errs <- admitErr
		}()
	}
	close(start)
	concurrent.Wait()
	close(ids)
	close(errs)
	var firstID uuid.UUID
	for id := range ids {
		if firstID == uuid.Nil {
			firstID = id
		} else if id != firstID {
			t.Fatalf("concurrent replay returned different IDs: %s %s", firstID, id)
		}
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var committedCount int
	var dbPath string
	if err := setup.QueryRow(ctx, `SELECT count(*),min(storage_path) FROM public.media_objects WHERE managed AND gateway_id=$1 AND upload_id=$2`, gw, req.UploadID).Scan(&committedCount, &dbPath); err != nil || committedCount != 1 || dbPath != firstID.String()+".mp4" {
		t.Fatalf("committed duplicate/path mismatch count=%d path=%q err=%v", committedCount, dbPath, err)
	}
	first := Record{MediaID: firstID, StoragePath: firstID.String() + ".mp4"}
	replay, err := repo.Admit(ctx, req)
	if err != nil || replay.MediaID != first.MediaID || replay.StoragePath != first.StoragePath {
		t.Fatalf("replay differs: %#v %v", replay, err)
	}
	changed := req
	changed.ExpectedSize++
	if _, err := repo.Admit(ctx, changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("metadata change: got %v", err)
	}
	restartedPool, err := pgxpool.New(ctx, os.Getenv("MEDIA_TEST_BACKEND_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer restartedPool.Close()
	restartedRepo, err := NewRepository(restartedPool, mediaLimits(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	afterRestart, err := restartedRepo.Lookup(ctx, gw, req.UploadID)
	if err != nil || afterRestart.MediaID != first.MediaID {
		t.Fatalf("persistent lookup: %#v %v", afterRestart, err)
	}
	foreign := mediaGateway(t, setup, "active")
	if _, err := repo.Lookup(ctx, foreign, req.UploadID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign active Gateway lookup must remain scoped: %v", err)
	}
	if _, err := setup.Exec(ctx, `UPDATE public.media_objects SET validation_status='rejected',terminal_reason='fixture_rejected' WHERE media_id=$1`, firstID); err != nil {
		t.Fatal(err)
	}
	terminalReplay, err := restartedRepo.Admit(ctx, req)
	if err != nil || terminalReplay.MediaID != firstID || terminalReplay.ValidationStatus != "rejected" ||
		terminalReplay.TerminalReason == nil || *terminalReplay.TerminalReason != "fixture_rejected" {
		t.Fatalf("terminal replay did not preserve committed record: %#v %v", terminalReplay, err)
	}
	if _, err := repo.Admit(ctx, mediaRequest(foreign, "fills_global_quota")); err != nil {
		t.Fatalf("fill unsettled quota: %v", err)
	}
	if _, err := repo.Admit(ctx, changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("metadata conflict must precede quota rejection: %v", err)
	}
	revoked := mediaGateway(t, setup, "revoked")
	seedMediaCredential(t, setup, revoked, true)
	if _, err := repo.Admit(ctx, mediaRequest(revoked, "denied")); !errors.Is(err, ErrForbidden) {
		t.Fatalf("revoked Gateway admission: %v", err)
	}
	if _, err := repo.Lookup(ctx, revoked, req.UploadID); !errors.Is(err, ErrForbidden) {
		t.Fatalf("revoked Gateway lookup: %v", err)
	}
}

func TestPostgresAdmissionSerializesQuotaAcrossGateways(t *testing.T) {
	setup, backend := requireMediaPostgres(t)
	limits := mediaLimits()
	limits.MaxUnsettledIntents = 1
	limits.TemporaryBytes = MaxUploadBytes
	repo, err := NewRepository(backend, limits, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	gateways := []string{mediaGateway(t, setup, "active"), mediaGateway(t, setup, "active")}
	start := make(chan struct{})
	results := make(chan error, len(gateways))
	var wg sync.WaitGroup
	for i, gw := range gateways {
		wg.Add(1)
		go func(i int, gw string) {
			defer wg.Done()
			<-start
			_, err := repo.Admit(context.Background(), mediaRequest(gw, fmt.Sprintf("race_%d", i)))
			results <- err
		}(i, gw)
	}
	close(start)
	wg.Wait()
	close(results)
	admitted, quota := 0, 0
	for err := range results {
		switch {
		case err == nil:
			admitted++
		case errors.Is(err, ErrQuota):
			quota++
		default:
			t.Fatalf("unexpected admission result: %v", err)
		}
	}
	if admitted != 1 || quota != 1 {
		t.Fatalf("admitted=%d quota=%d, want one each", admitted, quota)
	}
}

func TestPostgresAdmissionDeferredCommitFailureReturnsNoIntent(t *testing.T) {
	setup, backend := requireMediaPostgres(t)
	repo, err := NewRepository(backend, mediaLimits(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	gw := mediaGateway(t, setup, "active")
	ctx := context.Background()
	// A deferred constraint trigger makes the INSERT succeed but COMMIT fail,
	// exercising the ambiguous-commit boundary without modifying shared schema.
	fn := "media_commit_fail_" + uuid.NewString()[:8]
	trig := "media_commit_fail_" + uuid.NewString()[:8]
	_, err = setup.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION public.%s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture commit failure'; END $$`, fn))
	if err != nil {
		t.Fatal(err)
	}
	_, err = setup.Exec(ctx, fmt.Sprintf(`CREATE CONSTRAINT TRIGGER %s AFTER INSERT ON public.media_objects DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.%s()`, trig, fn))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = setup.Exec(context.Background(), fmt.Sprintf(`DROP TRIGGER IF EXISTS %s ON public.media_objects`, trig))
		_, _ = setup.Exec(context.Background(), fmt.Sprintf(`DROP FUNCTION IF EXISTS public.%s()`, fn))
	}()
	req := mediaRequest(gw, "commit_failure")
	record, err := repo.Admit(ctx, req)
	if record.MediaID != uuid.Nil || !errors.Is(err, ErrDependency) {
		t.Fatalf("commit error returned record/grant: %#v %v", record, err)
	}
	if record.MediaID != uuid.Nil {
		t.Fatalf("commit failure returned a usable media ID: %s", record.MediaID)
	}
	if _, err = repo.Lookup(ctx, gw, req.UploadID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed commit left visible intent: %v", err)
	}
}

func TestPostgresAdmissionUnsettledQuotaFencingAcrossLifecycle(t *testing.T) {
	setup, backend := requireMediaPostgres(t)
	limits := mediaLimits()
	limits.MaxPendingPerGateway = 1
	limits.MaxUnsettledIntents = 4
	limits.TemporaryBytes = 4 * MaxUploadBytes
	repo, err := NewRepository(backend, limits, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	gwA := mediaGateway(t, setup, "active")
	gwB := mediaGateway(t, setup, "active")
	ctx := context.Background()

	// 1. Gateway A admits upload A1
	reqA1 := mediaRequest(gwA, "upload_A1")
	recA1, err := repo.Admit(ctx, reqA1)
	if err != nil {
		t.Fatalf("admit A1: %v", err)
	}

	// 2. Cycle A1 to terminal 'rejected' with unconfirmed cleanup
	if _, err := setup.Exec(ctx, `UPDATE public.media_objects
		SET validation_status='rejected', terminal_reason='invalid_media', cleanup_scheduled_at=now()+interval '1 hour'
		WHERE media_id=$1`, recA1.MediaID); err != nil {
		t.Fatal(err)
	}

	// Gateway A tries to admit a new upload A2 with new upload_id
	// Under 1-slot per-Gateway, A must be quota-denied because A1 is still unsettled (unconfirmed cleanup).
	reqA2 := mediaRequest(gwA, "upload_A2")
	if _, err := repo.Admit(ctx, reqA2); !errors.Is(err, ErrQuota) {
		t.Fatalf("expected ErrQuota for Gateway A while A1 is uncleared rejected; got %v", err)
	}

	// Verify no new durable row or bytes were inserted for A2
	var countA int
	if err := setup.QueryRow(ctx, `SELECT COUNT(*) FROM public.media_objects WHERE gateway_id=$1 AND upload_id=$2`, gwA, reqA2.UploadID).Scan(&countA); err != nil || countA != 0 {
		t.Fatalf("unauthorized row created for A2: count=%d err=%v", countA, err)
	}

	// Gateway B with its own active credential can still admit
	reqB := mediaRequest(gwB, "upload_B")
	recB, err := repo.Admit(ctx, reqB)
	if err != nil {
		t.Fatalf("gateway B admission should succeed: %v", err)
	}
	if recB.GatewayID != gwB {
		t.Fatalf("unexpected record for B: %+v", recB)
	}

	// Same-ID replay for A1 returns the terminal record before quota checking
	replayA1, err := repo.Admit(ctx, reqA1)
	if err != nil || replayA1.MediaID != recA1.MediaID || replayA1.ValidationStatus != "rejected" {
		t.Fatalf("same-ID replay failed: replay=%+v err=%v", replayA1, err)
	}

	// Changed metadata on same-ID must return ErrConflict before quota checking
	changedA1 := reqA1
	changedA1.ExpectedSize += 10
	if _, err := repo.Admit(ctx, changedA1); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed metadata must return ErrConflict: %v", err)
	}

	// 3. Also cover 'expired' status: cycle A1 to 'expired' with unconfirmed cleanup
	if _, err := setup.Exec(ctx, `UPDATE public.media_objects
		SET validation_status='expired', terminal_reason='intent_expired', cleanup_scheduled_at=now()+interval '1 hour'
		WHERE media_id=$1`, recA1.MediaID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Admit(ctx, reqA2); !errors.Is(err, ErrQuota) {
		t.Fatalf("expected ErrQuota for Gateway A while A1 is uncleared expired; got %v", err)
	}

	// 4. Confirm successful actual cleanup for A1
	// Using claim/follow-up or setting cleanup_confirmed_at consistent with migration constraints
	if _, err := setup.Exec(ctx, `UPDATE public.media_objects
		SET cleanup_confirmed_at=cleanup_scheduled_at+interval '1 second'
		WHERE media_id=$1`, recA1.MediaID); err != nil {
		t.Fatal(err)
	}

	// 5. Now Gateway A can use its slot again to admit A2
	recA2, err := repo.Admit(ctx, reqA2)
	if err != nil {
		t.Fatalf("admit A2 after cleanup confirmed: %v", err)
	}
	if recA2.UploadID != reqA2.UploadID {
		t.Fatalf("unexpected A2 record: %+v", recA2)
	}

	// Old record A1 must remain immutable and unchanged
	var oldA1 Record
	if oldA1, err = repo.Lookup(ctx, gwA, reqA1.UploadID); err != nil {
		t.Fatalf("lookup old A1: %v", err)
	}
	if oldA1.MediaID != recA1.MediaID || oldA1.ValidationStatus != "expired" || oldA1.TerminalReason == nil || *oldA1.TerminalReason != "intent_expired" {
		t.Fatalf("old A1 mutated: %+v", oldA1)
	}
}

func TestPostgresGrantReservationFenceAndConfirmation(t *testing.T) {
	setup, backend := requireMediaPostgres(t)
	repo, err := NewRepository(backend, mediaLimits(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	gw := mediaGateway(t, setup, "active")
	ctx := context.Background()
	record, err := repo.Admit(ctx, mediaRequest(gw, "grant_fence"))
	if err != nil {
		t.Fatal(err)
	}
	req := GrantRequest{GatewayID: gw, UploadID: record.UploadID, MediaID: record.MediaID,
		SigningTimeout: time.Second, GrantTTL: 10 * time.Second, InFlightGrace: time.Second}
	first, status, err := repo.ReserveGrant(ctx, req)
	if err != nil || status != GrantNeedUpload {
		t.Fatalf("reserve: status=%q err=%v", status, err)
	}
	if _, status, err = repo.ReserveGrant(ctx, req); err != nil || status != GrantRetryAfterCompletion {
		t.Fatalf("duplicate reservation: status=%q err=%v", status, err)
	}
	actualExpiry := time.Now().UTC().Add(10 * time.Second)
	ok, err := repo.ConfirmGrant(ctx, first, actualExpiry)
	if err != nil || !ok {
		t.Fatalf("confirm reserved issuance: ok=%v err=%v", ok, err)
	}
	stale := first
	stale.GrantGeneration++
	if ok, err = repo.ConfirmGrant(ctx, stale, actualExpiry); err != nil || ok {
		t.Fatalf("stale issuance confirmation: ok=%v err=%v", ok, err)
	}
	stale = first
	stale.MediaID = uuid.New()
	if ok, err = repo.ConfirmGrant(ctx, stale, actualExpiry); err != nil || ok {
		t.Fatalf("stale managed-row confirmation: ok=%v err=%v", ok, err)
	}
	if _, err := repo.Complete(ctx, gw, record.UploadID); err != nil {
		t.Fatalf("completion transition: %v", err)
	}
	if _, err := setup.Exec(ctx, `UPDATE public.media_objects SET verification_generation=verification_generation+1,verification_attempts=verification_attempts+1 WHERE media_id=$1`, record.MediaID); err != nil {
		t.Fatal(err)
	}
	again, err := repo.Complete(ctx, gw, record.UploadID)
	if err != nil || again.ValidationStatus != "verifying" || again.VerificationGeneration != record.VerificationGeneration+1 {
		t.Fatalf("duplicate completion reset/failed: %#v %v", again, err)
	}
	var attempts int
	if err := setup.QueryRow(ctx, `SELECT verification_attempts FROM public.media_objects WHERE media_id=$1`, record.MediaID).Scan(&attempts); err != nil || attempts != 1 {
		t.Fatalf("duplicate completion reset attempts=%d err=%v", attempts, err)
	}
	worker := VerificationGeneration{GatewayID: gw, UploadID: record.UploadID, MediaID: record.MediaID,
		Generation: record.VerificationGeneration + 1}
	if _, err := setup.Exec(ctx, `UPDATE public.media_objects SET next_verification_at=now()+interval '30 seconds' WHERE media_id=$1`, record.MediaID); err != nil {
		t.Fatal(err)
	}
	if err := repo.RecordMissing(ctx, worker); err != nil {
		t.Fatalf("defer missing observation until grant horizon: %v", err)
	}
	var deferred time.Time
	if err := setup.QueryRow(ctx, `SELECT next_verification_at FROM public.media_objects WHERE media_id=$1`, record.MediaID).Scan(&deferred); err != nil || !deferred.After(time.Now()) {
		t.Fatalf("missing observation was not durably deferred: %v %v", deferred, err)
	}
	var waitingAttempts int
	if err := setup.QueryRow(ctx, `SELECT verification_attempts FROM public.media_objects WHERE media_id=$1`, record.MediaID).Scan(&waitingAttempts); err != nil || waitingAttempts != 0 {
		t.Fatalf("pre-horizon wait burned an attempt: attempts=%d err=%v", waitingAttempts, err)
	}
	if _, err := setup.Exec(ctx, `UPDATE public.media_objects SET next_reconcile_at=now()-interval '1 second' WHERE media_id=$1`, record.MediaID); err != nil {
		t.Fatal(err)
	}
	if err := repo.RecordMissing(ctx, worker); err != nil {
		t.Fatalf("record confirmed missing: %v", err)
	}
	second, status, err := repo.ReserveGrant(ctx, req)
	if err != nil || status != GrantNeedUpload || second.GrantGeneration <= first.GrantGeneration {
		t.Fatalf("re-reserve after confirmed missing: %#v status=%q err=%v", second, status, err)
	}
}

func TestPostgresGrantDuplicatePreservesLivePendingIntent(t *testing.T) {
	setup, backend := requireMediaPostgres(t)
	repo, err := NewRepository(backend, mediaLimits(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	gw := mediaGateway(t, setup, "active")
	ctx := context.Background()

	// Branch 1: Pending intent with one committed reservation, old grant still eligible,
	// current time sufficiently before intent_expires_at for completion but too late
	// for a second full-horizon reservation.
	record, err := repo.Admit(ctx, mediaRequest(gw, "dup_preserve_intent"))
	if err != nil {
		t.Fatal(err)
	}
	req := GrantRequest{
		GatewayID: gw, UploadID: record.UploadID, MediaID: record.MediaID,
		SigningTimeout: 2 * time.Second, GrantTTL: 10 * time.Second, InFlightGrace: 2 * time.Second,
	}
	first, status, err := repo.ReserveGrant(ctx, req)
	if err != nil || status != GrantNeedUpload {
		t.Fatalf("initial reserve: status=%q err=%v", status, err)
	}
	actualExpiry := time.Now().UTC().Add(10 * time.Second)
	ok, err := repo.ConfirmGrant(ctx, first, actualExpiry)
	if err != nil || !ok {
		t.Fatalf("confirm grant: ok=%v err=%v", ok, err)
	}

	// Deterministically set intent_expires_at to now() + 6s, while next_reconcile_at is now() + 10s.
	// Current time is before intent_expires_at (6s remaining), but full new-grant horizon is 14s.
	if _, err := setup.Exec(ctx, `UPDATE public.media_objects
		SET intent_expires_at=now()+interval '6 seconds',
		    latest_upload_grant_expires_at=now()+interval '10 seconds',
		    next_reconcile_at=now()+interval '10 seconds'
		WHERE media_id=$1`, record.MediaID); err != nil {
		t.Fatal(err)
	}

	// Duplicate ReserveGrant must return URL-free GrantRetryAfterCompletion and empty reservation
	dupRes, dupStatus, err := repo.ReserveGrant(ctx, req)
	if err != nil || dupStatus != GrantRetryAfterCompletion || dupRes.GrantGeneration != 0 {
		t.Fatalf("duplicate ReserveGrant: status=%q reservation=%+v err=%v", dupStatus, dupRes, err)
	}

	// Verify DB state preserved: still pending, no terminal reason, no cleanup scheduled, generation unchanged
	var valStatus string
	var termReason, cleanSched *string
	var grantGen int64
	if err := setup.QueryRow(ctx, `SELECT validation_status, terminal_reason, cleanup_scheduled_at::text, upload_grant_generation
		FROM public.media_objects WHERE media_id=$1`, record.MediaID).Scan(&valStatus, &termReason, &cleanSched, &grantGen); err != nil {
		t.Fatal(err)
	}
	if valStatus != "pending" || termReason != nil || cleanSched != nil || grantGen != first.GrantGeneration {
		t.Fatalf("pending intent modified prematurely: status=%q termReason=%v cleanSched=%v grantGen=%d (want %d)",
			valStatus, termReason, cleanSched, grantGen, first.GrantGeneration)
	}

	// Complete can now be called before intent_expires_at and successfully transition to verifying
	completed, err := repo.Complete(ctx, gw, record.UploadID)
	if err != nil || completed.ValidationStatus != "verifying" {
		t.Fatalf("completion failed: status=%q err=%v", completed.ValidationStatus, err)
	}

	// Normal fenced verification can reach committed ready with same path
	claimed, err := repo.ClaimDueVerification(ctx, 1, 5*time.Second)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim verification: claimed=%+v err=%v", claimed, err)
	}
	ready, err := repo.FinishVerification(ctx, VerificationGeneration{
		GatewayID: gw, UploadID: record.UploadID, MediaID: record.MediaID, Generation: claimed[0].VerificationGeneration,
	}, &VerifiedEvidence{ActualSize: record.ExpectedSize, SHA256: record.DeclaredSHA256, MediaType: "video/mp4"}, "")
	if err != nil || ready.ValidationStatus != "ready" || ready.StoragePath != record.StoragePath {
		t.Fatalf("finish verification: ready=%+v err=%v", ready, err)
	}

	// Branch 2: Live pending with NO old grant but insufficient new-grant budget.
	// Must return GrantPending, no reservation, and NOT prematurely expire.
	noOldGrant := mediaRequest(gw, "no_old_grant_budget")
	rec2, err := repo.Admit(ctx, noOldGrant)
	if err != nil {
		t.Fatal(err)
	}
	// intent_expires_at is now() + 6s, horizon is 14s. No old grant (next_reconcile_at IS NULL).
	if _, err := setup.Exec(ctx, `UPDATE public.media_objects SET intent_expires_at=now()+interval '6 seconds' WHERE media_id=$1`, rec2.MediaID); err != nil {
		t.Fatal(err)
	}
	req2 := GrantRequest{
		GatewayID: gw, UploadID: rec2.UploadID, MediaID: rec2.MediaID,
		SigningTimeout: 2 * time.Second, GrantTTL: 10 * time.Second, InFlightGrace: 2 * time.Second,
	}
	res2, status2, err := repo.ReserveGrant(ctx, req2)
	if err != nil || status2 != GrantPending || res2.GrantGeneration != 0 {
		t.Fatalf("insufficient budget without old grant: status=%q res=%+v err=%v", status2, res2, err)
	}
	// Assert still pending in DB
	var statusInDB string
	if err := setup.QueryRow(ctx, `SELECT validation_status FROM public.media_objects WHERE media_id=$1`, rec2.MediaID).Scan(&statusInDB); err != nil || statusInDB != "pending" {
		t.Fatalf("record prematurely modified: status=%q err=%v", statusInDB, err)
	}

	// Branch 3: Genuinely elapsed deadline (intent_expires_at <= now).
	// Must return GrantExpired, mark expired, and schedule cleanup.
	if _, err := setup.Exec(ctx, `UPDATE public.media_objects
		SET created_at=now()-interval '2 seconds',intent_expires_at=now()-interval '1 second'
		WHERE media_id=$1`, rec2.MediaID); err != nil {
		t.Fatal(err)
	}
	res3, status3, err := repo.ReserveGrant(ctx, req2)
	if err != nil || status3 != GrantExpired || res3.GrantGeneration != 0 {
		t.Fatalf("genuinely elapsed intent: status=%q res=%+v err=%v", status3, res3, err)
	}
	var valStatus3 string
	var termReason3, cleanSched3 *string
	if err := setup.QueryRow(ctx, `SELECT validation_status, terminal_reason, cleanup_scheduled_at::text
		FROM public.media_objects WHERE media_id=$1`, rec2.MediaID).Scan(&valStatus3, &termReason3, &cleanSched3); err != nil {
		t.Fatal(err)
	}
	if valStatus3 != "expired" || termReason3 == nil || *termReason3 != "intent_expired" || cleanSched3 == nil {
		t.Fatalf("genuinely elapsed not expired: valStatus=%q termReason=%v cleanSched=%v", valStatus3, termReason3, cleanSched3)
	}

	// Branch 4: Expired verifying handling: if an upload entered verifying but intent_expires_at has elapsed,
	// ReserveGrant must return GrantExpired, mark expired, clear next_verification_at, and schedule cleanup.
	gw4 := mediaGateway(t, setup, "active")
	rec4, err := repo.Admit(ctx, mediaRequest(gw4, "expired_verifying"))
	if err != nil {
		t.Fatal(err)
	}
	resGrant4, stat4, err := repo.ReserveGrant(ctx, GrantRequest{
		GatewayID: gw4, UploadID: rec4.UploadID, MediaID: rec4.MediaID,
		SigningTimeout: time.Second, GrantTTL: 10 * time.Second, InFlightGrace: time.Second,
	})
	if err != nil || stat4 != GrantNeedUpload {
		t.Fatalf("reserve for verifying: stat=%q err=%v", stat4, err)
	}
	if ok, err := repo.ConfirmGrant(ctx, resGrant4, time.Now().UTC().Add(10*time.Second)); err != nil || !ok {
		t.Fatalf("confirm for verifying: ok=%v err=%v", ok, err)
	}
	verifyingRec, err := repo.Complete(ctx, gw4, rec4.UploadID)
	if err != nil || verifyingRec.ValidationStatus != "verifying" {
		t.Fatalf("complete to verifying: status=%q err=%v", verifyingRec.ValidationStatus, err)
	}
	if _, err := setup.Exec(ctx, `UPDATE public.media_objects
		SET created_at=now()-interval '2 seconds',intent_expires_at=now()-interval '1 second'
		WHERE media_id=$1`, rec4.MediaID); err != nil {
		t.Fatal(err)
	}
	res4, stat4, err := repo.ReserveGrant(ctx, GrantRequest{
		GatewayID: gw4, UploadID: rec4.UploadID, MediaID: rec4.MediaID,
		SigningTimeout: time.Second, GrantTTL: 10 * time.Second, InFlightGrace: time.Second,
	})
	if err != nil || stat4 != GrantExpired || res4.GrantGeneration != 0 {
		t.Fatalf("expired verifying should return GrantExpired: stat=%q res=%+v err=%v", stat4, res4, err)
	}
	var valStatus4 string
	var termReason4, cleanSched4 *string
	var nextVerif4 *time.Time
	if err := setup.QueryRow(ctx, `SELECT validation_status, terminal_reason, cleanup_scheduled_at::text, next_verification_at
		FROM public.media_objects WHERE media_id=$1`, rec4.MediaID).Scan(&valStatus4, &termReason4, &cleanSched4, &nextVerif4); err != nil {
		t.Fatal(err)
	}
	if valStatus4 != "expired" || termReason4 == nil || *termReason4 != "intent_expired" || cleanSched4 == nil || nextVerif4 != nil {
		t.Fatalf("expired verifying DB mismatch: valStatus=%q termReason=%v cleanSched=%v nextVerif=%v",
			valStatus4, termReason4, cleanSched4, nextVerif4)
	}
}

func TestPostgresGrantDenialAndTerminalReplay(t *testing.T) {
	setup, backend := requireMediaPostgres(t)
	repo, err := NewRepository(backend, mediaLimits(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	revoked := mediaGateway(t, setup, "revoked")
	seedMediaCredential(t, setup, revoked, true)
	if _, err := repo.Admit(context.Background(), mediaRequest(revoked, "grant_denied")); !errors.Is(err, ErrForbidden) {
		t.Fatalf("revoked admission: %v", err)
	}
	gw := mediaGateway(t, setup, "active")
	revocable, err := repo.Admit(context.Background(), mediaRequest(gw, "grant_revoked"))
	if err != nil {
		t.Fatal(err)
	}
	revokeMediaCredential(t, setup, gw)
	revokedGrant := GrantRequest{GatewayID: gw, UploadID: revocable.UploadID, MediaID: revocable.MediaID,
		SigningTimeout: time.Second, GrantTTL: time.Second}
	if _, _, err := repo.ReserveGrant(context.Background(), revokedGrant); !errors.Is(err, ErrForbidden) {
		t.Fatalf("revoked Gateway grant reservation: %v", err)
	}
	if _, err := setup.Exec(context.Background(), `DELETE FROM public.media_objects WHERE media_id=$1`, revocable.MediaID); err != nil {
		t.Fatal(err)
	}

	gw = mediaGateway(t, setup, "active")
	record, err := repo.Admit(context.Background(), mediaRequest(gw, "grant_expired"))
	if err != nil {
		t.Fatal(err)
	}
	req := GrantRequest{GatewayID: gw, UploadID: record.UploadID, MediaID: record.MediaID,
		SigningTimeout: time.Second, GrantTTL: 10 * time.Second, InFlightGrace: time.Second}
	if _, status, err := repo.ReserveGrant(context.Background(), req); err != nil || status != GrantNeedUpload {
		t.Fatalf("initial grant: status=%q err=%v", status, err)
	}
	if _, err := setup.Exec(context.Background(), `UPDATE public.media_objects SET validation_status='rejected',terminal_reason='fixture_rejected' WHERE media_id=$1`, record.MediaID); err != nil {
		t.Fatal(err)
	}
	if _, status, err := repo.ReserveGrant(context.Background(), req); err != nil || status != GrantRejected {
		t.Fatalf("terminal grant replay: status=%q err=%v", status, err)
	}
	gwDeadline := mediaGateway(t, setup, "active")
	short := mediaRequest(gwDeadline, "grant_deadline")
	deadlineRecord, err := repo.Admit(context.Background(), short)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := setup.Exec(context.Background(), `UPDATE public.media_objects SET intent_expires_at=now()+interval '2 seconds' WHERE media_id=$1`, deadlineRecord.MediaID); err != nil {
		t.Fatal(err)
	}
	shortReq := GrantRequest{GatewayID: gwDeadline, UploadID: deadlineRecord.UploadID, MediaID: deadlineRecord.MediaID,
		SigningTimeout: time.Second, GrantTTL: 10 * time.Second}
	if _, status, err := repo.ReserveGrant(context.Background(), shortReq); err != nil || status != GrantPending {
		t.Fatalf("insufficient remaining deadline: status=%q err=%v", status, err)
	}
	if _, err := setup.Exec(context.Background(), `UPDATE public.media_objects
		SET created_at=now()-interval '2 seconds',intent_expires_at=now()-interval '1 second'
		WHERE media_id=$1`, deadlineRecord.MediaID); err != nil {
		t.Fatal(err)
	}
	if _, status, err := repo.ReserveGrant(context.Background(), shortReq); err != nil || status != GrantExpired {
		t.Fatalf("genuinely elapsed deadline: status=%q err=%v", status, err)
	}
}

func TestPostgresCompleteExpiredSchedulesSafeCleanup(t *testing.T) {
	setup, backend := requireMediaPostgres(t)
	repo, err := NewRepository(backend, mediaLimits(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	gw := mediaGateway(t, setup, "active")
	record, err := repo.Admit(context.Background(), mediaRequest(gw, "complete_expired"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := setup.Exec(context.Background(), `UPDATE public.media_objects
 SET created_at=now()-interval '2 seconds',intent_expires_at=now()-interval '1 second'
 WHERE media_id=$1`, record.MediaID); err != nil {
		t.Fatal(err)
	}
	got, err := repo.Complete(context.Background(), gw, record.UploadID)
	if err != nil || got.ValidationStatus != "expired" || got.TerminalReason == nil || *got.TerminalReason != "intent_expired" {
		t.Fatalf("expired completion: %#v %v", got, err)
	}
	var cleanup, deadline time.Time
	if err := setup.QueryRow(context.Background(), `SELECT cleanup_scheduled_at,intent_expires_at FROM public.media_objects WHERE media_id=$1`, record.MediaID).Scan(&cleanup, &deadline); err != nil || cleanup.Before(deadline) {
		t.Fatalf("cleanup boundary=%s deadline=%s err=%v", cleanup, deadline, err)
	}
}

func TestPostgresExpiryAndCleanupClaimsPreserveTerminalAndExcludeReady(t *testing.T) {
	setup, backend := requireMediaPostgres(t)
	limits := mediaLimits()
	limits.MaxUnsettledIntents = 1
	limits.TemporaryBytes = MaxUploadBytes
	repo, err := NewRepository(backend, limits, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	gw := mediaGateway(t, setup, "active")
	expired, err := repo.Admit(ctx, mediaRequest(gw, "cleanup_expired"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = setup.Exec(ctx, `UPDATE public.media_objects SET created_at=now()-interval '3 seconds',
 intent_expires_at=now()-interval '1 second',
 latest_upload_grant_expires_at=now()-interval '1 second' WHERE media_id=$1`, expired.MediaID); err != nil {
		t.Fatal(err)
	}
	if n, err := repo.ExpireDue(ctx, 1); err != nil || n != 1 {
		t.Fatalf("expire n=%d err=%v", n, err)
	}
	if _, err = setup.Exec(ctx, `UPDATE public.media_objects SET next_cleanup_at=now()-interval '1 second' WHERE media_id=$1`, expired.MediaID); err != nil {
		t.Fatal(err)
	}
	claims, err := repo.ClaimDueCleanup(ctx, 1, time.Minute)
	if err != nil || len(claims) != 1 || claims[0].Path != expired.StoragePath {
		t.Fatalf("claim=%#v err=%v", claims, err)
	}
	if err = repo.ScheduleCleanupRetry(ctx, claims[0], time.Second, "FOLLOWUP_WINDOW"); err != nil {
		t.Fatal(err)
	}
	if _, err = setup.Exec(ctx, `UPDATE public.media_objects SET next_cleanup_at=now()-interval '1 second' WHERE media_id=$1`, expired.MediaID); err != nil {
		t.Fatal(err)
	}
	claims, err = repo.ClaimDueCleanup(ctx, 1, time.Minute)
	if err != nil || len(claims) != 1 || claims[0].Attempt != 2 || !claims[0].FollowupDue {
		t.Fatalf("followup claim=%#v err=%v", claims, err)
	}
	staleClaim := claims[0]
	staleClaim.Attempt = 1
	if err = repo.ConfirmCleanup(ctx, staleClaim); !errors.Is(err, ErrStaleOperation) {
		t.Fatalf("stale attempt confirm succeeded: %v", err)
	}
	if err = repo.ConfirmCleanup(ctx, claims[0]); err != nil {
		t.Fatal(err)
	}
	var status string
	if err = setup.QueryRow(ctx, `SELECT validation_status FROM public.media_objects WHERE media_id=$1`, expired.MediaID).Scan(&status); err != nil || status != "expired" {
		t.Fatalf("terminal result changed: %q err=%v", status, err)
	}
	replay, err := repo.Complete(ctx, gw, expired.UploadID)
	if err != nil || replay.ValidationStatus != "expired" || replay.TerminalReason == nil || *replay.TerminalReason != "intent_expired" {
		t.Fatalf("terminal replay changed after cleanup: %#v err=%v", replay, err)
	}
	if _, err = repo.Admit(ctx, mediaRequest(gw, "cleanup_quota_released")); err != nil {
		t.Fatalf("quota remained held after confirmed cleanup: %v", err)
	}
	// Raise the test-only cap after proving that one held slot was released so
	// a second managed record can exercise ready-row exclusion.
	repo.limits.MaxUnsettledIntents = 2
	repo.limits.MaxPendingPerGateway = 2
	repo.limits.TemporaryBytes = 2 * MaxUploadBytes
	readyReq := mediaRequest(gw, "cleanup_ready")
	digest := sha256.Sum256([]byte("ready"))
	readyReq.ExpectedSize, readyReq.DeclaredSHA256 = int64(len("ready")), hex.EncodeToString(digest[:])
	ready, err := repo.Admit(ctx, readyReq)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = setup.Exec(ctx, `UPDATE public.media_objects SET validation_status='ready',
 actual_size=expected_size,actual_media_type=media_type,verified_sha256=declared_sha256,
 created_at=now()-interval '3 seconds',intent_expires_at=now()-interval '1 second'
 WHERE media_id=$1`, ready.MediaID); err != nil {
		t.Fatal(err)
	}
	if n, err := repo.ExpireDue(ctx, 1); err != nil || n != 0 {
		t.Fatalf("ready row expired: n=%d err=%v", n, err)
	}
	if claims, err := repo.ClaimDueCleanup(ctx, 1, time.Minute); err != nil || len(claims) != 0 {
		t.Fatalf("ready object entered cleanup: claims=%#v err=%v", claims, err)
	}
	var readyStatus string
	if err = setup.QueryRow(ctx, `SELECT validation_status FROM public.media_objects WHERE media_id=$1`, ready.MediaID).Scan(&readyStatus); err != nil || readyStatus != "ready" {
		t.Fatalf("ready changed to %q err=%v", readyStatus, err)
	}
}

func TestPostgresVerifierClaimFencingReadyReplayMissingReplayAndRetry(t *testing.T) {
	setup, backend := requireMediaPostgres(t)
	limits := mediaLimits()
	limits.MaxUnsettledIntents = 8
	limits.TemporaryBytes = 8 * MaxUploadBytes
	repo, err := NewRepository(backend, limits, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	lease := 30 * time.Second

	// A second process can reclaim an expired lease. The old generation cannot
	// finish, while the current generation commits ready and replays durably.
	gw := mediaGateway(t, setup, "active")
	sum := sha256.Sum256([]byte("sample"))
	req := mediaRequest(gw, "verify_ready")
	req.ExpectedSize, req.DeclaredSHA256 = int64(len("sample")), hex.EncodeToString(sum[:])
	rec, err := repo.Admit(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = repo.Complete(ctx, gw, rec.UploadID); err != nil {
		t.Fatal(err)
	}
	first, err := repo.ClaimDueVerification(ctx, 1, lease)
	if err != nil || len(first) != 1 || first[0].VerificationAttempts != 1 {
		t.Fatalf("first claim: %#v err=%v", first, err)
	}
	if _, err = setup.Exec(ctx, `UPDATE public.media_objects SET next_verification_at=now()-interval '1 second' WHERE media_id=$1`, rec.MediaID); err != nil {
		t.Fatal(err)
	}
	second, err := repo.ClaimDueVerification(ctx, 1, lease)
	if err != nil || len(second) != 1 || second[0].VerificationGeneration <= first[0].VerificationGeneration {
		t.Fatalf("restart reclaim: %#v err=%v", second, err)
	}
	oldClaim := VerificationGeneration{GatewayID: gw, UploadID: rec.UploadID, MediaID: rec.MediaID, Generation: first[0].VerificationGeneration}
	if _, err = repo.FinishVerification(ctx, oldClaim, &VerifiedEvidence{ActualSize: req.ExpectedSize, SHA256: req.DeclaredSHA256, MediaType: "video/mp4"}, ""); !errors.Is(err, ErrStaleOperation) {
		t.Fatalf("stale generation finished: %v", err)
	}
	current := VerificationGeneration{GatewayID: gw, UploadID: rec.UploadID, MediaID: rec.MediaID, Generation: second[0].VerificationGeneration}
	ready, err := repo.FinishVerification(ctx, current, &VerifiedEvidence{ActualSize: req.ExpectedSize, SHA256: req.DeclaredSHA256, MediaType: "video/mp4"}, "")
	if err != nil || ready.ValidationStatus != "ready" {
		t.Fatalf("ready commit: %#v err=%v", ready, err)
	}
	replay, err := repo.Complete(ctx, gw, rec.UploadID)
	if err != nil || replay.ValidationStatus != "ready" {
		t.Fatalf("ready replay: %#v err=%v", replay, err)
	}
	var actual int64
	if err = setup.QueryRow(ctx, `SELECT actual_size FROM public.media_objects WHERE media_id=$1`, rec.MediaID).Scan(&actual); err != nil || actual != req.ExpectedSize {
		t.Fatalf("verified actual size=%d err=%v", actual, err)
	}

	// An invalid but completely read object can only become rejected.
	gw = mediaGateway(t, setup, "active")
	bad := mediaRequest(gw, "verify_invalid")
	rec, err = repo.Admit(ctx, bad)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = repo.Complete(ctx, gw, rec.UploadID); err != nil {
		t.Fatal(err)
	}
	claimRows, err := repo.ClaimDueVerification(ctx, 1, lease)
	if err != nil || len(claimRows) != 1 {
		t.Fatalf("invalid claim: %#v err=%v", claimRows, err)
	}
	claim := VerificationGeneration{GatewayID: gw, UploadID: rec.UploadID, MediaID: rec.MediaID, Generation: claimRows[0].VerificationGeneration}
	rejected, err := repo.FinishVerification(ctx, claim, nil, "INVALID_MP4")
	if err != nil || rejected.ValidationStatus != "rejected" {
		t.Fatalf("invalid completion: %#v err=%v", rejected, err)
	}
	var rejectedActual *int64
	if err = setup.QueryRow(ctx, `SELECT actual_size FROM public.media_objects WHERE media_id=$1`, rec.MediaID).Scan(&rejectedActual); err != nil || rejectedActual != nil {
		t.Fatalf("invalid content stored ready metadata: actual=%v err=%v", rejectedActual, err)
	}
	// Missing object after its safe grant horizon becomes a durable pending
	// instruction; duplicate upload_complete must replay it, not restart work.
	gw = mediaGateway(t, setup, "active")
	missing, err := repo.Admit(ctx, mediaRequest(gw, "verify_missing"))
	if err != nil {
		t.Fatal(err)
	}
	reservation, status, err := repo.ReserveGrant(ctx, GrantRequest{GatewayID: gw, UploadID: missing.UploadID,
		MediaID: missing.MediaID, SigningTimeout: time.Second, GrantTTL: 10 * time.Second})
	if err != nil || status != GrantNeedUpload {
		t.Fatalf("missing grant reservation: %q %v", status, err)
	}
	if ok, err := repo.ConfirmGrant(ctx, reservation, time.Now().Add(5*time.Second)); err != nil || !ok {
		t.Fatalf("missing grant confirmation: %v %v", ok, err)
	}
	if _, err = repo.Complete(ctx, gw, missing.UploadID); err != nil {
		t.Fatal(err)
	}
	if _, err = setup.Exec(ctx, `UPDATE public.media_objects SET next_reconcile_at=now()-interval '1 second' WHERE media_id=$1`, missing.MediaID); err != nil {
		t.Fatal(err)
	}
	missClaims, err := repo.ClaimDueVerification(ctx, 1, lease)
	if err != nil || len(missClaims) != 1 {
		t.Fatalf("missing claim: %#v err=%v", missClaims, err)
	}
	missClaim := VerificationGeneration{GatewayID: gw, UploadID: missing.UploadID, MediaID: missing.MediaID, Generation: missClaims[0].VerificationGeneration}
	missingResult, publishable, err := repo.RecordMissingResult(ctx, missClaim)
	if err != nil || !publishable || missingResult.ValidationStatus != "pending" || missingResult.LastVerificationError != "OBJECT_MISSING" {
		t.Fatalf("durable missing result: %#v publish=%v err=%v", missingResult, publishable, err)
	}
	missingReplay, err := repo.Complete(ctx, gw, missing.UploadID)
	if err != nil || missingReplay.ValidationStatus != "pending" || missingReplay.LastVerificationError != "OBJECT_MISSING" {
		t.Fatalf("missing replay lost instruction: %#v err=%v", missingReplay, err)
	}
	nextGrant, nextStatus, err := repo.ReserveGrant(ctx, GrantRequest{GatewayID: gw, UploadID: missing.UploadID,
		MediaID: missing.MediaID, SigningTimeout: time.Second, GrantTTL: 10 * time.Second})
	if err != nil || nextStatus != GrantNeedUpload {
		t.Fatalf("regrant after confirmed missing: status=%q err=%v", nextStatus, err)
	}
	withoutGrant, err := repo.Complete(ctx, gw, missing.UploadID)
	if err != nil || withoutGrant.ValidationStatus != "pending" || withoutGrant.LastVerificationError != "OBJECT_MISSING" {
		t.Fatalf("reservation without grant cleared missing instruction: %#v err=%v", withoutGrant, err)
	}
	if ok, err := repo.ConfirmGrant(ctx, nextGrant, time.Now().Add(5*time.Second)); err != nil || !ok {
		t.Fatalf("confirm next grant: valid=%v err=%v", ok, err)
	}
	afterGrant, err := repo.Complete(ctx, gw, missing.UploadID)
	if err != nil || afterGrant.ValidationStatus != "verifying" || afterGrant.LastVerificationError != "" {
		t.Fatalf("complete after grant did not restart verification: %#v err=%v", afterGrant, err)
	}

	// Transient Storage failure has a finite attempt budget and a distinct
	// terminal reason from invalid media.
	gw = mediaGateway(t, setup, "active")
	transient, err := repo.Admit(ctx, mediaRequest(gw, "verify_retry"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = repo.Complete(ctx, gw, transient.UploadID); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		if _, err = setup.Exec(ctx, `UPDATE public.media_objects SET next_verification_at=now()-interval '1 second' WHERE media_id=$1`, transient.MediaID); err != nil {
			t.Fatal(err)
		}
		rows, claimErr := repo.ClaimDueVerification(ctx, 1, lease)
		if claimErr != nil || len(rows) != 1 {
			t.Fatalf("retry claim %d: %#v err=%v", attempt, rows, claimErr)
		}
		retryClaim := VerificationGeneration{GatewayID: gw, UploadID: transient.UploadID, MediaID: transient.MediaID, Generation: rows[0].VerificationGeneration}
		transient, err = repo.VerificationRetry(ctx, retryClaim, "STORAGE_UNAVAILABLE", 2, time.Second)
		if err != nil {
			t.Fatalf("retry transition %d: %v", attempt, err)
		}
	}
	if transient.ValidationStatus != "rejected" || transient.TerminalReason == nil || *transient.TerminalReason != "verification_exhausted" {
		t.Fatalf("retry exhaustion: %#v", transient)
	}
}

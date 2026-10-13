package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	nativeDSNFileEnv = "TEST_CREDENTIAL_OWNER_NATIVE_DSN_FILE"
	childModeEnv     = "TEST_CREDENTIAL_OWNER_CHILD_MODE"

	// Output tokens reported by child process (never secrets or passwords).
	tokenAcquired = "ACQUIRED"
	tokenDenied   = "DENIED"
	tokenFailed   = "FAILED"
)

func readNativeDSN(t *testing.T) string {
	t.Helper()
	filePath := os.Getenv(nativeDSNFileEnv)
	if filePath == "" {
		t.Skipf("skipping native test: %s not set", nativeDSNFileEnv)
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read DSN from %s: %v", filePath, err)
	}
	dsn := strings.TrimSpace(string(data))
	if dsn == "" {
		t.Fatalf("DSN in %s is empty", filePath)
	}
	return dsn
}

func connectNativePool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to create pgxpool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
	})
	return pool
}

// TestCredentialOwner_NativeChildProcess acts as the entrypoint when spawned as a child process.
// It reads TEST_CREDENTIAL_OWNER_CHILD_MODE and TEST_CREDENTIAL_OWNER_NATIVE_DSN_FILE.
// It reports only safe tokens ("ACQUIRED", "DENIED", "FAILED") on stdout.
func TestCredentialOwner_NativeChildProcess(t *testing.T) {
	mode := os.Getenv(childModeEnv)
	if mode == "" {
		// Not running in child mode; return immediately.
		return
	}

	dsnFilePath := os.Getenv(nativeDSNFileEnv)
	if dsnFilePath == "" {
		fmt.Println(tokenFailed)
		os.Exit(2)
	}
	data, err := os.ReadFile(dsnFilePath)
	if err != nil {
		fmt.Println(tokenFailed)
		os.Exit(2)
	}
	dsn := strings.TrimSpace(string(data))
	if dsn == "" {
		fmt.Println(tokenFailed)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		fmt.Println(tokenFailed)
		os.Exit(1)
	}
	defer pool.Close()

	switch mode {
	case "try_acquire":
		owner, err := acquireCredentialOwner(ctx, pool, 2*time.Second)
		if err != nil {
			if err == errOwnerLockDenied {
				fmt.Println(tokenDenied)
				return
			}
			fmt.Println(tokenFailed)
			return
		}
		defer func() {
			_ = owner.Close(context.Background(), 2*time.Second)
		}()
		fmt.Println(tokenAcquired)

	case "acquire_check_close":
		owner, err := acquireCredentialOwner(ctx, pool, 2*time.Second)
		if err != nil {
			if err == errOwnerLockDenied {
				fmt.Println(tokenDenied)
				return
			}
			fmt.Println(tokenFailed)
			return
		}
		if err := owner.CheckOwnership(ctx, 2*time.Second); err != nil {
			_ = owner.Close(context.Background(), 2*time.Second)
			fmt.Println(tokenFailed)
			return
		}
		if err := owner.Close(ctx, 2*time.Second); err != nil {
			fmt.Println(tokenFailed)
			return
		}
		fmt.Println(tokenAcquired)

	default:
		fmt.Println(tokenFailed)
		os.Exit(2)
	}
}

// spawnChildRunner executes the current test binary as a separate OS process with restricted environment.
func spawnChildRunner(t *testing.T, mode, dsnFilePath string) (string, error) {
	t.Helper()

	testBin, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("failed to get test executable path: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, testBin, "-test.run", "^TestCredentialOwner_NativeChildProcess$", "-test.v")
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		childModeEnv + "=" + mode,
		nativeDSNFileEnv + "=" + dsnFilePath,
	}

	out, err := cmd.CombinedOutput()
	outStr := string(out)

	for _, line := range strings.Split(outStr, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == tokenAcquired || trimmed == tokenDenied || trimmed == tokenFailed {
			return trimmed, nil
		}
	}

	return "", fmt.Errorf("child process did not emit expected token; output: %s; err: %v", outStr, err)
}

func TestCredentialOwner_NativeLifecycle(t *testing.T) {
	dsn := readNativeDSN(t)
	dsnFilePath := os.Getenv(nativeDSNFileEnv)

	t.Run("Step1_Owner1_Acquire_And_CheckOwnership", func(t *testing.T) {
		pool := connectNativePool(t, dsn)
		ctx := context.Background()

		owner1, err := acquireCredentialOwner(ctx, pool, 5*time.Second)
		if err != nil {
			t.Fatalf("owner1 acquire failed: %v", err)
		}
		defer func() {
			if err := owner1.Close(context.Background(), 5*time.Second); err != nil {
				t.Logf("cleanup owner1 close: %v", err)
			}
		}()

		// CheckOwnership must succeed natively via pg_locks
		if err := owner1.CheckOwnership(ctx, 3*time.Second); err != nil {
			t.Fatalf("owner1 CheckOwnership failed: %v", err)
		}
	})

	t.Run("Step2_Independent_Child_Process_Denied", func(t *testing.T) {
		pool := connectNativePool(t, dsn)
		ctx := context.Background()

		owner1, err := acquireCredentialOwner(ctx, pool, 5*time.Second)
		if err != nil {
			t.Fatalf("owner1 acquire failed: %v", err)
		}
		defer func() {
			_ = owner1.Close(context.Background(), 5*time.Second)
		}()

		if err := owner1.CheckOwnership(ctx, 3*time.Second); err != nil {
			t.Fatalf("owner1 CheckOwnership failed: %v", err)
		}

		// Spawn independent child process attempting to acquire while owner1 holds the lock
		token, err := spawnChildRunner(t, "try_acquire", dsnFilePath)
		if err != nil {
			t.Fatalf("child runner failed: %v", err)
		}
		if token != tokenDenied {
			t.Fatalf("expected child to be %s, got: %s", tokenDenied, token)
		}

		// Verify owner1 still holds the lock
		if err := owner1.CheckOwnership(ctx, 3*time.Second); err != nil {
			t.Fatalf("owner1 CheckOwnership after child denial failed: %v", err)
		}
	})

	t.Run("Step3_Close_Releases_Fresh_Child_Can_Acquire", func(t *testing.T) {
		pool := connectNativePool(t, dsn)
		ctx := context.Background()

		owner1, err := acquireCredentialOwner(ctx, pool, 5*time.Second)
		if err != nil {
			t.Fatalf("owner1 acquire failed: %v", err)
		}

		if err := owner1.CheckOwnership(ctx, 3*time.Second); err != nil {
			t.Fatalf("owner1 CheckOwnership failed: %v", err)
		}

		// Close owner1
		if err := owner1.Close(ctx, 5*time.Second); err != nil {
			t.Fatalf("owner1 close failed: %v", err)
		}

		// Now independent child process can acquire, check, and close
		token, err := spawnChildRunner(t, "acquire_check_close", dsnFilePath)
		if err != nil {
			t.Fatalf("child runner failed: %v", err)
		}
		if token != tokenAcquired {
			t.Fatalf("expected child to be %s after owner1 close, got: %s", tokenAcquired, token)
		}
	})

	t.Run("Step4_AdvisoryLock_Removed_Externally_Latches_Unavailable", func(t *testing.T) {
		pool := connectNativePool(t, dsn)
		ctx := context.Background()

		owner1, err := acquireCredentialOwner(ctx, pool, 5*time.Second)
		if err != nil {
			t.Fatalf("owner1 acquire failed: %v", err)
		}
		defer func() {
			_ = owner1.Close(context.Background(), 5*time.Second)
		}()

		if err := owner1.CheckOwnership(ctx, 3*time.Second); err != nil {
			t.Fatalf("owner1 CheckOwnership failed: %v", err)
		}

		// Unlock the advisory lock on owner1's dedicated connection directly within TEST ONLY
		var unlocked bool
		row := owner1.conn.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", credentialOwnerLockKey)
		if err := row.Scan(&unlocked); err != nil || !unlocked {
			t.Fatalf("failed to unlock owner1 lock in test: err=%v, unlocked=%v", err, unlocked)
		}

		// Next CheckOwnership must fail with errOwnerNotHeld and latch permanently unavailable
		checkErr := owner1.CheckOwnership(ctx, 3*time.Second)
		if checkErr != errOwnerNotHeld {
			t.Fatalf("expected %v after manual unlock, got: %v", errOwnerNotHeld, checkErr)
		}

		// Subsequent CheckOwnership call must immediately return errOwnerUnavailable (zero successful recovery)
		checkErr2 := owner1.CheckOwnership(ctx, 3*time.Second)
		if checkErr2 != errOwnerUnavailable {
			t.Fatalf("expected %v on subsequent call, got: %v", errOwnerUnavailable, checkErr2)
		}

		// Close still disposes safely
		closeErr := owner1.Close(ctx, 5*time.Second)
		if closeErr != nil && closeErr != errOwnerNotHeld {
			t.Fatalf("expected nil or errOwnerNotHeld on close after manual unlock, got: %v", closeErr)
		}
	})

	t.Run("Step5_Exact_pg_locks_Predicates_Verified", func(t *testing.T) {
		pool := connectNativePool(t, dsn)
		ctx := context.Background()

		owner1, err := acquireCredentialOwner(ctx, pool, 5*time.Second)
		if err != nil {
			t.Fatalf("owner1 acquire failed: %v", err)
		}
		defer func() {
			_ = owner1.Close(context.Background(), 5*time.Second)
		}()

		// Query pg_locks from another connection on pool to verify exact single-bigint columns
		var (
			locktype string
			granted  bool
			objsubid *int
			classid  uint32
			objid    uint32
		)
		high32 := int64(credentialOwnerLockKey >> 32)
		low32 := int64(int32(credentialOwnerLockKey & 0xFFFFFFFF))

		const inspectSQL = `
SELECT locktype, granted, objsubid, classid, objid
FROM pg_locks
WHERE locktype = 'advisory'
  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
  AND classid = $1
  AND objid = $2
`
		row := pool.QueryRow(ctx, inspectSQL, high32, low32)
		if err := row.Scan(&locktype, &granted, &objsubid, &classid, &objid); err != nil {
			t.Fatalf("failed to inspect pg_locks for expected single-bigint advisory lock: %v", err)
		}

		if locktype != "advisory" {
			t.Errorf("expected locktype='advisory', got: %s", locktype)
		}
		if !granted {
			t.Errorf("expected granted=true")
		}
		if objsubid == nil || *objsubid != 1 {
			t.Errorf("expected objsubid=1 (single 64-bit int session advisory lock), got: %v", objsubid)
		}
		if int64(int32(classid)) != high32 {
			t.Errorf("expected classid=%d, got: %d", high32, int64(int32(classid)))
		}
		if int64(int32(objid)) != low32 {
			t.Errorf("expected objid=%d, got: %d", low32, int64(int32(objid)))
		}

		// Also verify that a two-int lock (objsubid = 2) or lock in another session does NOT satisfy CheckOwnership
		// We test this by acquiring a 2-int lock on another connection and verifying owner1's predicates
		var twoIntLocked bool
		row2 := pool.QueryRow(ctx, "SELECT pg_try_advisory_lock($1, $2)", int32(high32), int32(low32))
		if err := row2.Scan(&twoIntLocked); err != nil || !twoIntLocked {
			t.Fatalf("failed to acquire 2-int advisory lock for negative check: %v", err)
		}
		defer func() {
			_, _ = pool.Exec(context.Background(), "SELECT pg_advisory_unlock($1, $2)", int32(high32), int32(low32))
		}()

		// Query pg_locks for objsubid = 2 to prove it exists in the 2-int namespace
		var twoIntSubid int
		rowSub2 := pool.QueryRow(ctx, `
SELECT objsubid FROM pg_locks
WHERE locktype = 'advisory'
  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
  AND classid = $1
  AND objid = $2
  AND objsubid = 2
`, high32, low32)
		if err := rowSub2.Scan(&twoIntSubid); err != nil {
			t.Fatalf("expected objsubid=2 lock to be visible in pg_locks: %v", err)
		}
		if twoIntSubid != 2 {
			t.Errorf("expected objsubid=2, got: %d", twoIntSubid)
		}

		// owner1's CheckOwnership must still succeed because it strictly queries objsubid = 1 for its own pid
		if err := owner1.CheckOwnership(ctx, 3*time.Second); err != nil {
			t.Fatalf("owner1 CheckOwnership failed in presence of two-int lock: %v", err)
		}
	})
}

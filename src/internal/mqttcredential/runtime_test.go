//nolint:errcheck,misspell // Negative filesystem fixtures and Mosquitto product paths are intentional.
package mqttcredential

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// Child-only seams terminate at persistence boundaries, without production hooks.
func runtimeCrashHelper(dir, point string) {
	exe, _ := os.Executable()
	r, err := NewRuntime(RuntimeConfig{AuthDir: dir, Tool: NativeTool{Path: exe}, Reload: successfulProbe, RecoveryProbe: func(context.Context, string, string) error { return nil }})
	if err != nil {
		os.Exit(11)
	}
	crash := func() { _ = syscall.Kill(os.Getpid(), syscall.SIGKILL); select {} }
	rename := r.ops.rename
	r.ops.rename = func(a, b string) error {
		if filepath.Base(a) == "candidate" && point == "beforeRename" {
			crash()
		}
		if filepath.Base(a) == "last-good" && point == "beforePromotion" {
			crash()
		}
		err := rename(a, b)
		if err == nil && filepath.Base(a) == "candidate" && point == "afterRename" {
			crash()
		}
		return err
	}
	r.config.Reload = func(context.Context) error {
		if point == "afterReload" {
			crash()
		}
		return nil
	}
	_ = r.RemoveGatewayCredential(context.Background(), "crashed", "gw_test", successfulProbe)
}

func TestRuntimeProcessCrashFailClosed(t *testing.T) {
	for _, point := range []string{"beforeRename", "afterRename", "afterReload", "beforePromotion"} {
		t.Run(point, func(t *testing.T) {
			r := runtimeFixture(t)
			old := []byte(testFile())
			if err := os.WriteFile(r.path("passwd"), old, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(r.path("passwd.last-good"), old, 0600); err != nil {
				t.Fatal(err)
			}
			exe, _ := os.Executable()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := exec.CommandContext(ctx, exe, "runtime-crash", r.config.AuthDir, point).Run()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL || ctx.Err() != nil {
				t.Fatalf("child did not reach crash boundary: %v", err)
			}
			live, err := os.ReadFile(r.path("passwd"))
			if err != nil {
				t.Fatal(err)
			}
			if (point == "beforeRename") != bytes.Equal(live, old) {
				t.Fatal("wrong live snapshot at crash")
			}
			fresh, _ := NewRuntime(r.config)
			if !errors.Is(fresh.CheckRuntime(context.Background()), ErrRecoveryRequired) {
				t.Fatal("restart ignored interrupted operation")
			}
			if !errors.Is(fresh.RemoveGatewayCredential(context.Background(), "blocked", "gw_test", successfulProbe), ErrRecoveryRequired) {
				t.Fatal("restart allowed mutation")
			}
			after, _ := os.ReadFile(r.path("passwd"))
			if !bytes.Equal(live, after) {
				t.Fatal("restart silently restored revoked account")
			}
			if err := os.Remove(r.path(".credential.pending")); err != nil {
				t.Fatal(err)
			}
			if !errors.Is(fresh.CheckRuntime(context.Background()), ErrRecoveryRequired) {
				t.Fatal("staging-only crash ignored")
			}
		})
	}
}

func TestRuntimeFaultMatrix(t *testing.T) {
	for _, fault := range []string{"malformed", "live-corrupt", "permissions", "file-ENOSPC", "candidate-fsync", "stage-fsync", "intent-dir-fsync", "rename", "live-dir-fsync", "rollback-rename", "promotion"} {
		t.Run(fault, func(t *testing.T) {
			r := runtimeFixture(t)
			old := []byte(testFile())
			if err := os.WriteFile(r.path("passwd.last-good"), old, 0600); err != nil {
				t.Fatal(err)
			}
			id := "gw_test"
			if fault == "malformed" {
				id = "gw_malformed"
			}
			if fault == "live-corrupt" {
				old = []byte("corrupt\n")
				if err := os.WriteFile(r.path("passwd"), old, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if fault == "permissions" {
				if err := os.Chmod(r.path("passwd"), 0644); err != nil {
					t.Fatal(err)
				}
			}
			syncFile := r.ops.syncFile
			var files int
			r.ops.syncFile = func(f *os.File) error {
				files++
				if fault == "file-ENOSPC" && files == 1 {
					return syscall.ENOSPC
				}
				if fault == "candidate-fsync" && files == 2 {
					return syscall.EIO
				}
				return syncFile(f)
			}
			syncDir := r.ops.syncDir
			var dirs int
			r.ops.syncDir = func(p string) error {
				dirs++
				if (fault == "stage-fsync" && dirs == 1) || (fault == "intent-dir-fsync" && dirs == 2) || (fault == "live-dir-fsync" && dirs == 3) {
					return syscall.EIO
				}
				return syncDir(p)
			}
			rename := r.ops.rename
			r.ops.rename = func(a, b string) error {
				if fault == "rename" && filepath.Base(a) == "candidate" {
					return syscall.EACCES
				}
				if fault == "rollback-rename" && filepath.Base(a) == "snapshot" {
					return syscall.EIO
				}
				if fault == "promotion" && filepath.Base(a) == "last-good" {
					return syscall.EIO
				}
				return rename(a, b)
			}
			probe := successfulProbe
			if fault == "rollback-rename" {
				probe = func(context.Context) error { return ErrVerificationFailed }
			}
			err := r.UpsertGatewayCredential(context.Background(), "fault", id, "next", probe)
			if err == nil {
				t.Fatal("fault accepted")
			}
			last, _ := os.ReadFile(r.path("passwd.last-good"))
			if !bytes.Equal(last, []byte(testFile())) {
				t.Fatal("fault replaced last-good")
			}
			live, _ := os.ReadFile(r.path("passwd"))
			uncertain := fault == "intent-dir-fsync" || fault == "rollback-rename" || fault == "promotion"
			if fault != "rollback-rename" && fault != "promotion" && !bytes.Equal(live, old) {
				t.Fatal("fault changed previous live store")
			}
			if uncertain {
				if !errors.Is(err, ErrRecoveryRequired) {
					t.Fatal(err)
				}
				fresh, _ := NewRuntime(r.config)
				if !errors.Is(fresh.CheckRuntime(context.Background()), ErrRecoveryRequired) {
					t.Fatal("durable evidence missing")
				}
			} else if errors.Is(err, ErrRecoveryRequired) {
				t.Fatal("recoverable fault poisoned store", err)
			}
		})
	}
}

func TestConcurrentRuntimeMutationsNoLostUpdate(t *testing.T) {
	for _, same := range []bool{false, true} {
		t.Run(fmt.Sprint(same), func(t *testing.T) {
			r := runtimeFixture(t)
			var wg sync.WaitGroup
			errs := make(chan error, 4)
			for i := 0; i < 4; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					fresh, _ := NewRuntime(r.config)
					id := fmt.Sprintf("gw_%d", i)
					if same {
						id = "gw_same"
					}
					errs <- fresh.UpsertGatewayCredential(context.Background(), fmt.Sprintf("op_%d", i), id, "concurrent-password", successfulProbe)
				}(i)
			}
			wg.Wait()
			close(errs)
			for e := range errs {
				if e != nil {
					t.Fatal(e)
				}
			}
			b, e := readSecure(r.path("passwd"), DefaultMaxFileBytes)
			if e != nil {
				t.Fatal(e)
			}
			parsed, e := ParsePasswordFile(bytes.NewReader(b), DefaultMaxFileBytes)
			if e != nil {
				t.Fatal(e)
			}
			want := 6
			if same {
				want = 3
			}
			if len(parsed) != want {
				t.Fatalf("lost update: %d entries, want %d", len(parsed), want)
			}
		})
	}
}

func TestConcurrentProcessMutationsNoLostUpdate(t *testing.T) {
	r := runtimeFixture(t)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var commands []*exec.Cmd
	for i := 0; i < 4; i++ {
		cmd := exec.CommandContext(ctx, exe, "runtime-update", r.config.AuthDir, fmt.Sprintf("gw_process_%d", i))
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		commands = append(commands, cmd)
	}
	for _, cmd := range commands {
		if err := cmd.Wait(); err != nil {
			t.Fatal(err)
		}
	}
	b, err := readSecure(r.path("passwd"), DefaultMaxFileBytes)
	if err != nil {
		t.Fatal(err)
	}
	users, err := ParsePasswordFile(bytes.NewReader(b), DefaultMaxFileBytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 6 {
		t.Fatal("cross-process mutation lost an account")
	}
	if err := r.CheckRuntime(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeCleanupFailureCannotReportSuccess(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission fault requires non-root UID")
	}
	r := runtimeFixture(t)
	stage := r.path(".op-cleanup")
	t.Cleanup(func() { _ = os.Chmod(stage, 0700) })
	rename := r.ops.rename
	r.ops.rename = func(a, b string) error {
		err := rename(a, b)
		if err == nil && b == r.path("passwd.last-good") {
			return os.Chmod(stage, 0500)
		}
		return err
	}
	err := r.UpsertGatewayCredential(context.Background(), "cleanup", "gw_test", "cleanup-test-password", successfulProbe)
	if !errors.Is(err, ErrRecoveryRequired) || !errors.Is(err, ErrPersistenceFailure) {
		t.Fatalf("cleanup failure reported successful completion: %v", err)
	}
	if _, e := os.Stat(r.path(".credential.pending")); e != nil {
		t.Fatal("cleanup failure missing durable marker")
	}
	fresh, _ := NewRuntime(r.config)
	if !errors.Is(fresh.CheckRuntime(context.Background()), ErrRecoveryRequired) {
		t.Fatal("restart ignored cleanup evidence")
	}
}

func TestRuntimeMarkerUnlinkDurabilityFailure(t *testing.T) {
	r := runtimeFixture(t)
	syncDir := r.ops.syncDir
	failed := false
	r.ops.syncDir = func(path string) error {
		// First directory fsync after marker unlink, before staging cleanup.
		if !failed && path == r.config.AuthDir {
			_, markerErr := os.Lstat(r.path(".credential.pending"))
			_, lastErr := os.Lstat(r.path("passwd.last-good"))
			if errors.Is(markerErr, os.ErrNotExist) && lastErr == nil {
				failed = true
				return syscall.EIO
			}
		}
		return syncDir(path)
	}
	if e := r.UpsertGatewayCredential(context.Background(), "unlink", "gw_test", "marker-test-password", successfulProbe); !errors.Is(e, ErrRecoveryRequired) || !failed {
		t.Fatal("uncertain marker unlink accepted", e)
	}
	fresh, _ := NewRuntime(r.config)
	if !errors.Is(fresh.CheckRuntime(context.Background()), ErrRecoveryRequired) {
		t.Fatal("marker unlink uncertainty lost on restart")
	}
}

func runtimeFixture(t *testing.T) *Runtime {
	t.Helper()
	dir := privateDir(t)
	if err := os.WriteFile(filepath.Join(dir, "passwd"), []byte(testFile()), 0600); err != nil {
		t.Fatal(err)
	}
	r, err := NewRuntime(RuntimeConfig{AuthDir: dir, Tool: helperTool(t), Reload: func(context.Context) error { return nil }, RecoveryProbe: func(context.Context, string, string) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func successfulProbe(context.Context) error { return nil }

func TestRecoveryRequiresSpecificEvidence(t *testing.T) {
	r := runtimeFixture(t)
	r.config.RecoveryProbe = nil
	if !errors.Is(r.RemoveGatewayCredential(context.Background(), "no-evidence", "gw_test", successfulProbe), ErrVerificationFailed) {
		t.Fatal("missing recovery evidence accepted")
	}
}

func TestRuntimeMutationAndGuard(t *testing.T) {
	r := runtimeFixture(t)
	ctx := context.Background()
	if err := r.CheckRuntime(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{BackendUsername, "bad/name"} {
		if err := r.UpsertGatewayCredential(ctx, "op", id, "test", successfulProbe); !errors.Is(err, ErrInvalidInput) {
			t.Fatal(err)
		}
	}
	for _, op := range []string{"../escape", "", "a/b", "x\x00"} {
		if err := r.RemoveGatewayCredential(ctx, op, "gw_test", successfulProbe); !errors.Is(err, ErrInvalidInput) {
			t.Fatal(err)
		}
	}
	if err := r.UpsertGatewayCredential(ctx, "rotate", "gw_test", "new-test", nil); !errors.Is(err, ErrVerificationFailed) {
		t.Fatal(err)
	}
	if err := r.UpsertGatewayCredential(ctx, "rotate", "gw_test", "new-test", successfulProbe); err != nil {
		t.Fatal(err)
	}
	last, err := readSecure(r.path("passwd.last-good"), DefaultMaxFileBytes)
	if err != nil {
		t.Fatal(err)
	}
	live, _ := readSecure(r.path("passwd"), DefaultMaxFileBytes)
	if string(last) != string(live) {
		t.Fatal("last-good differs")
	}
	if err := r.RemoveGatewayCredential(ctx, "remove", "gw_test", successfulProbe); err != nil {
		t.Fatal(err)
	}
	if err := r.CheckRuntime(ctx); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(r.config.AuthDir)
	for _, e := range entries {
		if e.Name() == ".credential.pending" || e.Name() == ".op-rotate" || e.Name() == ".op-remove" {
			t.Fatal("success artifacts retained")
		}
	}
}

func TestRuntimeRecovery(t *testing.T) {
	for _, failure := range []string{"probe", "reload", "recovery", "rename", "sync"} {
		t.Run(failure, func(t *testing.T) {
			r := runtimeFixture(t)
			old := testFile()
			var reloads int
			r.config.Reload = func(context.Context) error {
				reloads++
				if failure == "reload" && reloads == 1 {
					return errors.New("secret error")
				}
				return nil
			}
			verify := successfulProbe
			if failure == "probe" || failure == "recovery" {
				verify = func(context.Context) error { return errors.New("network is NOT auth denial") }
			}
			if failure == "recovery" {
				r.config.RecoveryProbe = func(context.Context, string, string) error { return errors.New("offline") }
			}
			if failure == "rename" {
				rename := r.ops.rename
				var count int
				r.ops.rename = func(a, b string) error {
					count++
					if count == 1 {
						return errors.New("injected")
					}
					return rename(a, b)
				}
			}
			if failure == "sync" {
				syncDir := r.ops.syncDir
				var count int
				r.ops.syncDir = func(path string) error {
					count++
					if count == 3 {
						return errors.New("injected")
					}
					return syncDir(path)
				}
			}
			err := r.UpsertGatewayCredential(context.Background(), "recovery-op", "gw_test", "new-test", verify)
			if err == nil {
				t.Fatal("failed mutation returned success")
			}
			live, _ := readSecure(r.path("passwd"), DefaultMaxFileBytes)
			if string(live) != old {
				t.Fatal("snapshot not restored")
			}
			if failure == "recovery" {
				if !errors.Is(err, ErrRecoveryRequired) {
					t.Fatal(err)
				}
				fresh, _ := NewRuntime(r.config)
				if !errors.Is(fresh.CheckRuntime(context.Background()), ErrRecoveryRequired) {
					t.Fatal("restart ignored pending marker")
				}
			} else {
				if errors.Is(err, ErrRecoveryRequired) {
					t.Fatal(err)
				}
				if err := r.CheckRuntime(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestRuntimeRejectChangedNonTargetAndPreRenameFailure(t *testing.T) {
	r := runtimeFixture(t)
	if err := r.UpsertGatewayCredential(context.Background(), "corrupt", "gw_corrupt", "test", successfulProbe); !errors.Is(err, ErrToolFailure) {
		t.Fatal(err)
	}
	r.ops.syncFile = func(*os.File) error { return errors.New("disk failure") }
	if err := r.UpsertGatewayCredential(context.Background(), "disk", "gw_test", "test", successfulProbe); !errors.Is(err, ErrPersistenceFailure) {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(r.path("passwd"))
	if string(b) != testFile() {
		t.Fatal("live file changed before rename")
	}
}

func TestRuntimeSerializesThroughProbeAndRecovery(t *testing.T) {
	r := runtimeFixture(t)
	r2, _ := NewRuntime(r.config)
	entered := make(chan struct{})
	unblock := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- r.UpsertGatewayCredential(context.Background(), "first", "gw_test", "new-test", func(context.Context) error { close(entered); <-unblock; return nil })
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if err := r2.RemoveGatewayCredential(ctx, "second", "gw_test", successfulProbe); !errors.Is(err, ErrRuntimeBusy) {
		t.Fatal(err)
	}
	close(unblock)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var recoveryCalled atomic.Bool
	r.config.RecoveryProbe = func(ctx context.Context, _, _ string) error {
		if ctx.Err() != nil {
			t.Error("recovery inherited cancellation")
		}
		recoveryCalled.Store(true)
		return nil
	}
	ctx, cancel = context.WithCancel(context.Background())
	err := r.UpsertGatewayCredential(ctx, "cancel", "gw_test", "next-test", func(context.Context) error { cancel(); return context.Canceled })
	if !errors.Is(err, ErrVerificationFailed) || !recoveryCalled.Load() {
		t.Fatal(err)
	}
}

func TestRuntimeMarkerAndSymlinkStartup(t *testing.T) {
	r := runtimeFixture(t)
	os.WriteFile(r.path(".credential.pending"), []byte("interrupted\n"), 0600)
	if !errors.Is(r.CheckRuntime(context.Background()), ErrRecoveryRequired) {
		t.Fatal("pending marker ignored")
	}
	os.Remove(r.path(".credential.pending"))
	os.Rename(r.path("passwd"), r.path("other"))
	os.Symlink(r.path("other"), r.path("passwd"))
	if !errors.Is(r.CheckRuntime(context.Background()), ErrPersistenceFailure) {
		t.Fatal("live symlink accepted")
	}
}

func TestLastGoodRetainedAndUncertainCommitBlocked(t *testing.T) {
	r := runtimeFixture(t)
	if err := r.UpsertGatewayCredential(context.Background(), "good", "gw_test", "new-test", successfulProbe); err != nil {
		t.Fatal(err)
	}
	last, _ := os.ReadFile(r.path("passwd.last-good"))
	if err := r.UpsertGatewayCredential(context.Background(), "failed", "gw_test", "next-test", func(context.Context) error { return context.DeadlineExceeded }); !errors.Is(err, ErrVerificationFailed) {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(r.path("passwd.last-good"))
	if string(after) != string(last) {
		t.Fatal("failed mutation replaced last-good")
	}
	rename := r.ops.rename
	r.ops.rename = func(a, b string) error {
		if b == r.path("passwd.last-good") {
			return errors.New("injected last-good failure")
		}
		return rename(a, b)
	}
	if err := r.UpsertGatewayCredential(context.Background(), "uncertain", "gw_test", "next-test", successfulProbe); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatal(err)
	}
	fresh, _ := NewRuntime(r.config)
	if !errors.Is(fresh.CheckRuntime(context.Background()), ErrRecoveryRequired) {
		t.Fatal("uncertain commit accepted after restart")
	}
}

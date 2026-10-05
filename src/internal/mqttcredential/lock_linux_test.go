package mqttcredential

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestLockAdmissionCancellation(t *testing.T) {
	dir := privateDir(t)
	release, err := acquireLock(context.Background(), dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		unlock, err := acquireLock(ctx, dir, 1)
		if err == nil {
			unlock()
		}
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	if unlock, err := acquireLock(context.Background(), dir, 1); !errors.Is(err, ErrRuntimeBusy) {
		if unlock != nil {
			unlock()
		}
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, ErrRuntimeBusy) {
		t.Fatal(err)
	}
	release()
	unlock, err := acquireLock(context.Background(), dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	unlock()
}

func TestLockAcrossProcess(t *testing.T) {
	dir := privateDir(t)
	release, err := acquireLock(context.Background(), dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	exe, _ := os.Executable()
	if err := exec.Command(exe, "lock-probe", filepath.Join(dir, ".credential.lock"), "true").Run(); err != nil {
		release()
		t.Fatal(err)
	}
	release()
	if err := exec.Command(exe, "lock-probe", filepath.Join(dir, ".credential.lock"), "false").Run(); err != nil {
		t.Fatal(err)
	}
}

package mqttcredential

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Initialize never repairs or rotates an existing store. An interrupted first
// initialization leaves explicit recovery evidence for an operator.
func Initialize(ctx context.Context, dir, password string, tool NativeTool) error {
	if ValidateBackendPassword(password) != nil || !filepath.IsAbs(tool.Path) || tool.Timeout <= 0 {
		return ErrInvalidInput
	}
	if err := secureDir(dir); err != nil {
		return err
	}
	release, err := acquireLock(ctx, dir, 8)
	if err != nil {
		return err
	}
	defer release()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return ErrPersistenceFailure
	}
	for _, e := range entries {
		if e.Name() != "passwd" && e.Name() != "passwd.last-good" && e.Name() != ".credential.lock" && !strings.HasPrefix(e.Name(), ".credential.slot.") {
			return ErrRecoveryRequired
		}
	}
	path := filepath.Join(dir, "passwd")
	if _, err := os.Lstat(path); err == nil {
		b, err := readSecure(path, DefaultMaxFileBytes)
		if err != nil {
			return err
		}
		users, err := ParsePasswordFile(bytes.NewReader(b), DefaultMaxFileBytes)
		if err != nil {
			return err
		}
		ok, err := VerifyPasswordHash(users[BackendUsername], password)
		if err != nil || !ok {
			return ErrInvalidInput
		}
		last, err := readSecure(filepath.Join(dir, "passwd.last-good"), DefaultMaxFileBytes)
		if err != nil || !bytes.Equal(b, last) {
			return ErrRecoveryRequired
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return ErrPersistenceFailure
	}
	if _, err := os.Lstat(filepath.Join(dir, "passwd.last-good")); !errors.Is(err, os.ErrNotExist) {
		return ErrRecoveryRequired
	}
	stage := filepath.Join(dir, ".op-initialize")
	if os.Mkdir(stage, 0700) != nil {
		return ErrPersistenceFailure
	}
	// Retain stage on any failure: a later start must not silently reset it.
	candidate := filepath.Join(stage, "candidate")
	if err := writeSnapshot(candidate, nil, defaultFileOps()); err != nil {
		return err
	}
	child, cancel := context.WithTimeout(ctx, tool.Timeout)
	defer cancel()
	cmd := exec.CommandContext(child, tool.Path, "-H", "sha512-pbkdf2", candidate, BackendUsername)
	cmd.Env = []string{"LC_ALL=C"}
	cmd.Stdin = strings.NewReader(password + "\n" + password + "\n")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	cmd.WaitDelay = 100 * time.Millisecond
	if cmd.Run() != nil {
		return ErrToolFailure
	}
	b, err := readSecure(candidate, DefaultMaxFileBytes)
	if err != nil {
		return err
	}
	users, err := ParsePasswordFile(bytes.NewReader(b), DefaultMaxFileBytes)
	if err != nil || len(users) != 1 {
		return ErrToolFailure
	}
	ok, err := VerifyPasswordHash(users[BackendUsername], password)
	if err != nil || !ok {
		return ErrToolFailure
	}
	if syncSecure(candidate, defaultFileOps()) != nil || os.Rename(candidate, path) != nil || syncDirectory(dir) != nil {
		return ErrPersistenceFailure
	}
	if err := writeSnapshot(filepath.Join(dir, "passwd.last-good"), b, defaultFileOps()); err != nil {
		return err
	}
	if syncDirectory(dir) != nil || os.RemoveAll(stage) != nil || syncDirectory(dir) != nil {
		return ErrPersistenceFailure
	}
	return nil
}

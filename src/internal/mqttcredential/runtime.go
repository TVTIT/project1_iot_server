//nolint:misspell // Native Mosquitto utility paths are product names.
package mqttcredential

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"iot-platform/internal/gateway"
)

// ProbeFunc verifies an operation through fresh authentication while holding the lock.
type ProbeFunc func(context.Context) error

// ReloadFunc requests broker signal delivery within its context deadline.
type ReloadFunc func(context.Context) error

// RuntimeConfig callbacks must obey context deadlines. Verify must prove this operation using
// fresh MQTT connections; a negative probe requires an exact auth rejection,
// never a timeout/network error. RecoveryProbe must prove the previous snapshot
// for this specific opID/target (including absence when provisioning a new user).
// The caller must retain that operation-specific evidence before invoking us.
type RuntimeConfig struct {
	AuthDir                                                        string
	Tool                                                           NativeTool
	OperationTimeout, ReloadTimeout, ProbeTimeout, RecoveryTimeout time.Duration
	MaxFileBytes                                                   int64
	MaxPending                                                     int
	Reload                                                         ReloadFunc
	RecoveryProbe                                                  func(ctx context.Context, opID, gatewayID string) error
}

// Runtime serializes atomic credential mutations and bounded rollback.
type Runtime struct {
	config   RuntimeConfig
	ops      fileOps
	poisoned bool // accessed only while holding the stable filesystem lock
}

var operationID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)

func validOpID(s string) bool { return operationID.MatchString(s) }

// NewRuntime supplies bounded engineering defaults, not benchmarked sizing.
// No environment is read here; later config/wiring owns deployment settings.
func NewRuntime(c RuntimeConfig) (*Runtime, error) {
	if c.OperationTimeout == 0 {
		c.OperationTimeout = 10 * time.Second
	}
	if c.ReloadTimeout == 0 {
		c.ReloadTimeout = 2 * time.Second
	}
	if c.ProbeTimeout == 0 {
		c.ProbeTimeout = 3 * time.Second
	}
	if c.RecoveryTimeout == 0 {
		c.RecoveryTimeout = 5 * time.Second
	}
	if c.Tool.Timeout == 0 {
		c.Tool.Timeout = 3 * time.Second
	}
	if c.Tool.Path == "" {
		c.Tool.Path = "/usr/bin/mosquitto_passwd"
	}
	if c.MaxFileBytes == 0 {
		c.MaxFileBytes = DefaultMaxFileBytes
	}
	if c.MaxPending == 0 {
		c.MaxPending = 8
	}
	if c.OperationTimeout < 0 || c.ReloadTimeout < 0 || c.ProbeTimeout < 0 || c.RecoveryTimeout < 0 || c.Tool.Timeout < 0 || c.MaxFileBytes < 1 || c.MaxFileBytes > 64<<20 || c.MaxPending < 0 || c.MaxPending > 1024 || !filepath.IsAbs(c.AuthDir) || filepath.Clean(c.AuthDir) != c.AuthDir {
		return nil, ErrInvalidInput
	}
	return &Runtime{config: c, ops: defaultFileOps()}, nil
}

func (r *Runtime) path(name string) string { return filepath.Join(r.config.AuthDir, name) }
func (r *Runtime) checkLocked() error {
	if r.poisoned {
		return ErrRecoveryRequired
	}
	entries, scanErr := os.ReadDir(r.config.AuthDir)
	if scanErr != nil {
		return ErrPersistenceFailure
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".op-") {
			return ErrRecoveryRequired
		}
	}
	if _, err := os.Lstat(r.path(".credential.pending")); !errors.Is(err, os.ErrNotExist) {
		return ErrRecoveryRequired
	}
	b, err := readSecure(r.path("passwd"), r.config.MaxFileBytes)
	if err != nil {
		return err
	}
	if _, err := ParsePasswordFile(bytes.NewReader(b), r.config.MaxFileBytes); err != nil {
		return err
	}
	if _, err := os.Lstat(r.path("passwd.last-good")); err == nil {
		last, err := readSecure(r.path("passwd.last-good"), r.config.MaxFileBytes)
		if err != nil || !bytes.Equal(last, b) {
			return ErrRecoveryRequired
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return ErrPersistenceFailure
	}
	return nil
}

// CheckRuntime validates the store and blocks unresolved recovery.
func (r *Runtime) CheckRuntime(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, r.config.OperationTimeout)
	defer cancel()
	if err := secureDir(r.config.AuthDir); err != nil {
		return err
	}
	release, err := acquireLock(ctx, r.config.AuthDir, r.config.MaxPending)
	if err != nil {
		return err
	}
	defer release()
	return r.checkLocked()
}

// UpsertGatewayCredential persists, reloads and verifies a Gateway credential.
func (r *Runtime) UpsertGatewayCredential(ctx context.Context, opID, id, password string, verify ProbeFunc) error {
	if !validPassword(password) {
		return ErrInvalidInput
	}
	return r.mutate(ctx, opID, id, password, false, verify)
}

// RemoveGatewayCredential removes a Gateway credential without touching the internal principal.
func (r *Runtime) RemoveGatewayCredential(ctx context.Context, opID, id string, verify ProbeFunc) error {
	return r.mutate(ctx, opID, id, "", true, verify)
}

func boundedCall(ctx context.Context, timeout time.Duration, fn ProbeFunc, category error) error {
	child, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if fn == nil {
		return category
	}
	if err := fn(child); err != nil || child.Err() != nil {
		return category
	}
	return nil
}

func (r *Runtime) mutate(ctx context.Context, opID, id, password string, remove bool, verify ProbeFunc) (result error) {
	if !validOpID(opID) || gateway.ValidateGatewayID(id) != nil {
		return ErrInvalidInput
	}
	if verify == nil || r.config.RecoveryProbe == nil {
		return ErrVerificationFailed
	}
	if r.config.Reload == nil {
		return ErrReloadUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, r.config.OperationTimeout)
	defer cancel()
	if err := secureDir(r.config.AuthDir); err != nil {
		return err
	}
	release, err := acquireLock(ctx, r.config.AuthDir, r.config.MaxPending)
	if err != nil {
		return err
	}
	defer release()
	if err := r.checkLocked(); err != nil {
		return err
	}
	old, err := readSecure(r.path("passwd"), r.config.MaxFileBytes)
	if err != nil {
		return err
	}
	before, err := ParsePasswordFile(bytes.NewReader(old), r.config.MaxFileBytes)
	if err != nil {
		return err
	}
	stage := r.path(".op-" + opID)
	if os.Mkdir(stage, 0700) != nil {
		return ErrPersistenceFailure
	}
	// A retained staging directory contains recovery evidence and is never reused.
	retain := false
	defer func() {
		if !retain {
			if os.RemoveAll(stage) != nil || r.ops.syncDir(r.config.AuthDir) != nil {
				// Cleanup is part of completion. Retained/uncertain staging must
				// never yield success now but fail startup on the next process.
				retain = true
				_ = writeSnapshot(r.path(".credential.pending"), []byte("recovery-required\n"), defaultFileOps())
				_ = syncDirectory(r.config.AuthDir)
				result = errors.Join(result, ErrPersistenceFailure, ErrRecoveryRequired)
			}
		}
		if retain {
			r.poisoned = true
		}
	}()
	candidate := filepath.Join(stage, "candidate")
	backup := filepath.Join(stage, "snapshot")
	if err := writeSnapshot(candidate, old, r.ops); err != nil {
		return err
	}
	if remove {
		err = r.config.Tool.NativeDelete(ctx, candidate, id)
	} else {
		err = r.config.Tool.NativeUpsert(ctx, candidate, id, password)
	}
	if err != nil {
		return err
	}
	updated, err := readSecure(candidate, r.config.MaxFileBytes)
	if err != nil {
		return err
	}
	after, err := ParsePasswordFile(bytes.NewReader(updated), r.config.MaxFileBytes)
	if err != nil {
		return ErrToolFailure
	}
	if err := validateMutation(before, after, id, remove); err != nil {
		return err
	}
	if !remove {
		ok, err := VerifyPasswordHash(after[id], password)
		if err != nil || !ok {
			return ErrToolFailure
		}
	}
	if ctx.Err() != nil {
		return ErrRuntimeBusy
	}
	if err := syncSecure(candidate, r.ops); err != nil {
		return err
	}
	if err := writeSnapshot(backup, old, r.ops); err != nil {
		return err
	}
	if r.ops.syncDir(stage) != nil {
		return ErrPersistenceFailure
	}
	// Durable intent precedes live rename. It contains no password or hash.
	if err := writeSnapshot(r.path(".credential.pending"), []byte(opID+"\n"), r.ops); err != nil {
		if _, e := os.Lstat(r.path(".credential.pending")); e == nil {
			retain = true
			return errors.Join(err, ErrRecoveryRequired)
		}
		return err
	}
	retain = true
	if r.ops.syncDir(r.config.AuthDir) != nil {
		return errors.Join(ErrPersistenceFailure, ErrRecoveryRequired)
	}
	if r.ops.rename(candidate, r.path("passwd")) != nil {
		return r.recover(opID, id, backup, ErrPersistenceFailure, &retain)
	}
	if r.ops.syncDir(r.config.AuthDir) != nil {
		return r.recover(opID, id, backup, ErrPersistenceFailure, &retain)
	}
	if err := boundedCall(ctx, r.config.ReloadTimeout, ProbeFunc(r.config.Reload), ErrReloadUnavailable); err != nil {
		return r.recover(opID, id, backup, err, &retain)
	}
	if err := boundedCall(ctx, r.config.ProbeTimeout, verify, ErrVerificationFailed); err != nil {
		return r.recover(opID, id, backup, err, &retain)
	}
	last := filepath.Join(stage, "last-good")
	if err := writeSnapshot(last, updated, r.ops); err != nil {
		return r.recover(opID, id, backup, err, &retain)
	}
	if r.ops.rename(last, r.path("passwd.last-good")) != nil || r.ops.syncDir(r.config.AuthDir) != nil {
		return errors.Join(ErrPersistenceFailure, ErrRecoveryRequired)
	}
	if err := r.clearMarker(); err != nil {
		return err
	}
	retain = false
	return nil
}

func (r *Runtime) clearMarker() error {
	if os.Remove(r.path(".credential.pending")) != nil || r.ops.syncDir(r.config.AuthDir) != nil {
		// Restore fail-closed evidence if unlink durability was uncertain. Never
		// silently regard a failed directory fsync as a completed operation.
		_ = writeSnapshot(r.path(".credential.pending"), []byte("recovery-required\n"), defaultFileOps())
		_ = syncDirectory(r.config.AuthDir)
		return errors.Join(ErrPersistenceFailure, ErrRecoveryRequired)
	}
	return nil
}

func (r *Runtime) recover(opID, id, backup string, cause error, retain *bool) error {
	// Independent budget: caller cancellation cannot skip rollback verification.
	ctx, cancel := context.WithTimeout(context.Background(), r.config.RecoveryTimeout)
	defer cancel()
	if r.ops.rename(backup, r.path("passwd")) != nil || r.ops.syncDir(r.config.AuthDir) != nil {
		return errors.Join(cause, ErrRecoveryRequired)
	}
	if boundedCall(ctx, r.config.ReloadTimeout, ProbeFunc(r.config.Reload), ErrReloadUnavailable) != nil {
		return errors.Join(cause, ErrRecoveryRequired)
	}
	probe := func(ctx context.Context) error { return r.config.RecoveryProbe(ctx, opID, id) }
	if boundedCall(ctx, r.config.ProbeTimeout, probe, ErrVerificationFailed) != nil {
		return errors.Join(cause, ErrRecoveryRequired)
	}
	if err := r.clearMarker(); err != nil {
		return errors.Join(cause, err)
	}
	*retain = false
	return cause // rollback is NOT success for the requested mutation
}

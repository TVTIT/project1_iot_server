package mqttcredential

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"iot-platform/internal/gateway"
)

// NativeTool operates only on a candidate in a private .op-<validated ID>
// directory. It never receives the live password-file path.
type NativeTool struct {
	Path    string
	Timeout time.Duration
}

// NativeUpsert hashes a credential in the private candidate using stdin only.
func (t NativeTool) NativeUpsert(ctx context.Context, candidate, username, password string) error {
	if !validPassword(password) {
		return ErrInvalidInput
	}
	return t.run(ctx, candidate, username, password, false)
}

// NativeDelete removes a principal from the private candidate.
func (t NativeTool) NativeDelete(ctx context.Context, candidate, username string) error {
	return t.run(ctx, candidate, username, "", true)
}

type boundedDiscard struct{ remaining int }

func (b *boundedDiscard) Write(p []byte) (int, error) {
	// Consume output without retaining sensitive content, even beyond the bound.
	if len(p) < b.remaining {
		b.remaining -= len(p)
	} else {
		b.remaining = 0
	}
	return len(p), nil
}

func (t NativeTool) run(ctx context.Context, candidate, user, password string, remove bool) error {
	if gateway.ValidateGatewayID(user) != nil || !filepath.IsAbs(t.Path) || filepath.Clean(t.Path) != t.Path {
		return ErrInvalidInput
	}
	dir := filepath.Dir(candidate)
	if filepath.Base(candidate) != "candidate" || !strings.HasPrefix(filepath.Base(dir), ".op-") || !validOpID(strings.TrimPrefix(filepath.Base(dir), ".op-")) {
		return ErrInvalidInput
	}
	if err := secureDir(dir); err != nil {
		return err
	}
	f, err := openSecure(candidate, syscall.O_RDWR)
	if err != nil {
		return err
	}
	_ = f.Close()
	timeout := t.Timeout
	if timeout == 0 {
		timeout = 3 * time.Second
	}
	if timeout < 0 {
		return ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	args := []string{"-H", "sha512-pbkdf2", candidate, user}
	if remove {
		args = []string{"-D", candidate, user}
	}
	cmd := exec.CommandContext(ctx, t.Path, args...)
	cmd.Env = []string{"LC_ALL=C"}
	cmd.Stdout = io.Discard
	cmd.Stderr = &boundedDiscard{remaining: 4096}
	if !remove {
		cmd.Stdin = strings.NewReader(password + "\n" + password + "\n")
	}
	// Kill the process group too; WaitDelay bounds inherited pipes on failure.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 100 * time.Millisecond
	err = cmd.Run()
	// Native .backup.XXXXXX artifacts are not authoritative snapshots.
	entries, scanErr := os.ReadDir(dir)
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "candidate.backup.") {
			if os.Remove(filepath.Join(dir, entry.Name())) != nil {
				scanErr = ErrPersistenceFailure
			}
		}
	}
	if err != nil || scanErr != nil {
		return ErrToolFailure
	}
	return nil
}

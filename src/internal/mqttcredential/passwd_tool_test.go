//nolint:errcheck // Child-process fault fixtures and descriptor cleanup are best effort.
package mqttcredential

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// A subprocess of this test binary supplies a hermetic native-tool stand-in.
// No production shell wrapper or executable dependency is introduced.
func TestMain(m *testing.M) {
	nativeHelper()
	os.Exit(m.Run())
}

func nativeHelper() {
	if len(os.Args) == 4 && os.Args[1] == "runtime-update" {
		exe, _ := os.Executable()
		r, err := NewRuntime(RuntimeConfig{AuthDir: os.Args[2], Tool: NativeTool{Path: exe}, Reload: successfulProbe, RecoveryProbe: func(context.Context, string, string) error { return nil }})
		if err != nil || r.UpsertGatewayCredential(context.Background(), os.Args[3], os.Args[3], "subprocess-test-password", successfulProbe) != nil {
			os.Exit(12)
		}
		os.Exit(0)
	}
	if len(os.Args) == 4 && os.Args[1] == "runtime-crash" {
		runtimeCrashHelper(os.Args[2], os.Args[3])
		os.Exit(10)
	}
	if len(os.Args) == 4 && os.Args[1] == "lock-probe" {
		fd, err := syscall.Open(os.Args[2], syscall.O_RDWR, 0600)
		if err != nil {
			os.Exit(9)
		}
		defer syscall.Close(fd)
		err = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB)
		wantBusy, _ := strconv.ParseBool(os.Args[3])
		if (err == syscall.EWOULDBLOCK) != wantBusy {
			os.Exit(8)
		}
		os.Exit(0)
	}
	if len(os.Args) < 4 || (os.Args[1] != "-H" && os.Args[1] != "-D") {
		return
	}
	remove := os.Args[1] == "-D"
	idx := 3
	if remove {
		idx = 2
	}
	file, user := os.Args[idx], os.Args[idx+1]
	if os.Getenv("TOOL_SECRET_CANARY") != "" || len(os.Environ()) != 1 || os.Getenv("LC_ALL") != "C" {
		os.Exit(9)
	}
	if user == "gw_slow" {
		time.Sleep(time.Second)
		os.Exit(0)
	}
	if user == "gw_error" {
		os.WriteFile(file+".backup.test", []byte("test"), 0600)
		os.Stderr.WriteString("do-not-leak-secret")
		os.Exit(3)
	}
	b, _ := os.ReadFile(file)
	var lines []string
	for _, line := range strings.Split(strings.TrimSuffix(string(b), "\n"), "\n") {
		if !strings.HasPrefix(line, user+":") {
			lines = append(lines, line)
		}
	}
	if !remove {
		input, _ := os.ReadFile("/dev/stdin")
		parts := strings.Split(string(input), "\n")
		if len(parts) != 3 || parts[0] != parts[1] || parts[2] != "" {
			os.Exit(8)
		}
		for _, arg := range os.Args {
			if strings.Contains(arg, parts[0]) {
				os.Exit(7)
			}
		}
		lines = append(lines, user+":"+testHash(parts[0]))
	}
	if user == "gw_corrupt" {
		lines[0] = BackendUsername + ":" + testHash("changed-backend")
	}
	if user == "gw_malformed" {
		lines = []string{"malformed"}
	}
	if os.WriteFile(file, []byte(strings.Join(lines, "\n")+"\n"), 0600) != nil {
		os.Exit(6)
	}
	os.Exit(0)
}

func privateDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}
func helperTool(t *testing.T) NativeTool {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return NativeTool{Path: exe, Timeout: 3 * time.Second}
}
func candidateFixture(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(privateDir(t), ".op-test")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "candidate")
	if err := os.WriteFile(path, []byte(testFile()), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestNativeTool(t *testing.T) {
	t.Setenv("TOOL_SECRET_CANARY", "must-not-inherit")
	tool, path := helperTool(t), candidateFixture(t)
	if err := tool.NativeUpsert(context.Background(), path, "gw_test", " test-canary "); err != nil {
		t.Fatal(err)
	}
	if err := tool.NativeDelete(context.Background(), path, "gw_test"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{BackendUsername, "bad/name"} {
		if err := tool.NativeUpsert(context.Background(), path, id, "test"); !errors.Is(err, ErrInvalidInput) {
			t.Fatal(err)
		}
	}
	for _, p := range []string{"x\n", "x\x00", ""} {
		if err := tool.NativeUpsert(context.Background(), path, "gw_test", p); !errors.Is(err, ErrInvalidInput) {
			t.Fatal(err)
		}
	}
	if err := tool.NativeUpsert(context.Background(), filepath.Join(filepath.Dir(path), "passwd"), "gw_test", "test"); !errors.Is(err, ErrInvalidInput) {
		t.Fatal(err)
	}
	if err := tool.NativeUpsert(context.Background(), path, "gw_error", "test"); !errors.Is(err, ErrToolFailure) || strings.Contains(err.Error(), "do-not-leak") {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".backup.test"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("native backup retained")
	}
	tool.Timeout = 30 * time.Millisecond
	start := time.Now()
	if err := tool.NativeUpsert(context.Background(), path, "gw_slow", "test"); !errors.Is(err, ErrToolFailure) {
		t.Fatal(err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("tool timeout not bounded")
	}
}

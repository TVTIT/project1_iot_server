//nolint:errcheck // Intentionally invalid filesystem fixtures are checked by Initialize.
package mqttcredential

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestInitialize(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	tool := filepath.Join(t.TempDir(), "tool")
	content := BackendUsername + ":" + testHash("fixture-password") + "\n"
	os.WriteFile(tool, []byte("#!/bin/sh\nprintf '%s\\n' '"+content[:len(content)-1]+"' > \"$3\"\n"), 0700)
	native := NativeTool{Path: tool, Timeout: time.Second}
	if err := Initialize(context.Background(), dir, "fixture-password", native); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(filepath.Join(dir, "passwd"))
	if err := Initialize(context.Background(), dir, "fixture-password", native); err != nil {
		t.Fatal(err)
	}
	if err := Initialize(context.Background(), dir, "wrong", native); err == nil {
		t.Fatal("rotated")
	}
	after, _ := os.ReadFile(filepath.Join(dir, "passwd"))
	if string(before) != string(after) {
		t.Fatal("changed")
	}
	for _, bad := range []string{".credential.pending", ".op-interrupted", "unknown"} {
		p := filepath.Join(dir, bad)
		os.WriteFile(p, []byte("pending"), 0600)
		if err := Initialize(context.Background(), dir, "fixture-password", native); err == nil {
			t.Fatal("ignored recovery evidence")
		}
		os.Remove(p)
	}
	os.WriteFile(filepath.Join(dir, "passwd"), []byte("corrupt"), 0600)
	if err := Initialize(context.Background(), dir, "fixture-password", native); err == nil {
		t.Fatal("reset corrupt store")
	}
}

func TestInitializePermissionsAndToolFailure(t *testing.T) {
	dir := t.TempDir()
	native := NativeTool{Path: "/missing-tool", Timeout: time.Second}
	if Initialize(context.Background(), dir, "", native) == nil {
		t.Fatal("empty password")
	}
	os.Chmod(dir, 0755)
	if Initialize(context.Background(), dir, "fixture-password", native) == nil {
		t.Fatal("unsafe permissions")
	}
	os.Chmod(dir, 0700)
	if Initialize(context.Background(), dir, "fixture-password", native) == nil {
		t.Fatal("missing tool")
	}
	if Initialize(context.Background(), dir, "fixture-password", native) == nil {
		t.Fatal("ignored interrupted stage")
	}
}

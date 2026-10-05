//nolint:errcheck // Intentionally invalid filesystem fixtures are checked by Initialize.
package mqttcredential

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestInitializeRejectsUnsafeBackendPasswordBeforeFilesystemChanges(t *testing.T) {
	for _, password := range []string{"replace_with_backend_mqtt_password", "REPLACE_WITH_secret", "change_me", " \t ", "\u2003", "", "a\rb", "a\nb", "a\x00b"} {
		for _, existing := range []bool{false, true} {
			t.Run("invalid_backend_password", func(t *testing.T) {
				dir := t.TempDir()
				os.Chmod(dir, 0700)
				before := []byte(BackendUsername + ":" + testHash("fixture-password") + "\n")
				if existing {
					os.WriteFile(filepath.Join(dir, "passwd"), before, 0600)
					os.WriteFile(filepath.Join(dir, "passwd.last-good"), before, 0600)
				}
				// A launched tool leaves evidence, even if its output is invalid.
				called := filepath.Join(t.TempDir(), "called")
				tool := filepath.Join(t.TempDir(), "tool")
				os.WriteFile(tool, []byte("#!/bin/sh\ntouch '"+called+"'\nexit 1\n"), 0700)
				if err := Initialize(context.Background(), dir, password, NativeTool{Path: tool, Timeout: time.Second}); !errors.Is(err, ErrInvalidInput) {
					t.Fatalf("expected invalid input, got %v", err)
				}
				if _, err := os.Stat(called); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("utility was invoked")
				}
				entries, _ := os.ReadDir(dir)
				want := 0
				if existing {
					want = 2
					for _, name := range []string{"passwd", "passwd.last-good"} {
						after, _ := os.ReadFile(filepath.Join(dir, name))
						if string(after) != string(before) {
							t.Fatal("existing store changed")
						}
					}
				}
				if len(entries) != want {
					t.Fatal("invalid password changed directory entries")
				}
			})
		}
	}
}

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

func TestInitializePreservesLiteralPassword(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	password := " literal password "
	tool := filepath.Join(t.TempDir(), "tool")
	content := BackendUsername + ":" + testHash(password)
	// The utility stand-in must receive both lines verbatim, not trimmed.
	os.WriteFile(tool, []byte("#!/bin/sh\nIFS= read -r first\nIFS= read -r second\n[ \"$first\" = ' literal password ' ] && [ \"$second\" = ' literal password ' ] || exit 1\nprintf '%s\\n' '"+content+"' > \"$3\"\n"), 0700)
	native := NativeTool{Path: tool, Timeout: time.Second}
	if err := Initialize(context.Background(), dir, password, native); err != nil {
		t.Fatal(err)
	}
	if err := Initialize(context.Background(), dir, password, native); err != nil {
		t.Fatal("verbatim password did not verify on retry")
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

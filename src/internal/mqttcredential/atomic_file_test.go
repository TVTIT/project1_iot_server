//nolint:errcheck // Filesystem fault fixtures are validated by the operation under test.
package mqttcredential

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSecureFilesAndDurability(t *testing.T) {
	dir := privateDir(t)
	path := filepath.Join(dir, "snapshot")
	ops := defaultFileOps()
	if err := writeSnapshot(path, []byte("test"), ops); err != nil {
		t.Fatal(err)
	}
	if err := writeSnapshot(path, []byte("other"), ops); err == nil {
		t.Fatal("overwrote snapshot")
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := readSecure(path, 10); err == nil {
		t.Fatal("accepted public file")
	}
	os.Chmod(path, 0600)
	link := filepath.Join(dir, "link")
	os.Symlink(path, link)
	if _, err := readSecure(link, 10); err == nil {
		t.Fatal("followed symlink")
	}
	alias := filepath.Join(dir, "alias")
	os.Link(path, alias)
	if _, err := readSecure(path, 10); err == nil {
		t.Fatal("accepted hardlink")
	}
	if err := secureDir(dir); err != nil {
		t.Fatal(err)
	}
	dirLink := filepath.Join(privateDir(t), "link")
	os.Symlink(dir, dirLink)
	if err := secureDir(dirLink); err == nil {
		t.Fatal("accepted directory symlink")
	}
	ops.syncFile = func(*os.File) error { return errors.New("injected") }
	if err := writeSnapshot(filepath.Join(dir, "failed"), []byte("test"), ops); !errors.Is(err, ErrPersistenceFailure) {
		t.Fatal(err)
	}
}

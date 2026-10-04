package mqttcredential

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDynSecNativeProjection(t *testing.T) {
	for _, raw := range []string{`{"clients":[{"username":"A","password":"SECRET","salt":"HASH","roles":[]}]}`, `{"clients":[{"username":"A","disabled":true,"roles":[{"rolename":"gateway_A","priority":1}]}]}`} {
		v, e := parseDynSecSnapshot([]byte(raw), "A")
		if e != nil || v.Username != "A" {
			t.Fatalf("native: %+v %v", v, e)
		}
	}
	for _, raw := range []string{`{}`, `{"clients":null}`, `{"clients":[{"username":"A","disabled":null}]}`, `{"clients":[{"username":"A","disabled":"false"}]}`, `{"clients":[{"username":"A","disabled":true,"disabled":false}]}`, `{"clients":[{"username":"A"},{"username":"A"}]}`, `{"clients":[{"username":"B"}]}`, `{"clients":[{"username":"A","roles":[{"rolename":null}]}]}`, `{"clients":[]} {}`, `{"clients":[{"username":"A","unknown":{"x":1,"x":2}}]}`} {
		if _, e := parseDynSecSnapshot([]byte(raw), "A"); e == nil {
			t.Fatalf("accepted invalid %s", raw)
		}
	}
}
func TestDynSecProtectedSnapshot(t *testing.T) {
	dir := t.TempDir()
	_ = os.Chmod(dir, 0700)
	path := filepath.Join(dir, "native.json")
	raw := `{"clients":[{"username":"A","disabled":false}]}`
	cfg := DynSecSnapshotConfig{Path: path, WriterUID: uint32(os.Getuid()), MaxBytes: 512, ValidateTarget: func(s string) bool { return s == "A" }}
	r, e := NewDynSecSnapshotReader(cfg)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = r.Observe(context.Background(), "A"); e == nil {
		t.Fatal("missing")
	}
	if e = os.WriteFile(path, []byte(raw), 0600); e != nil {
		t.Fatal(e)
	}
	v, e := r.Observe(context.Background(), "A")
	if e != nil || v.Disabled {
		t.Fatalf("observation %v %v", v, e)
	}
	if _, e = r.Observe(context.Background(), "../A"); e == nil {
		t.Fatal("untrusted target")
	}
	for _, mode := range []os.FileMode{0644, 0660, 0000} {
		_ = os.Chmod(path, mode)
		if _, e = r.Observe(context.Background(), "A"); e == nil {
			t.Fatalf("permissions %o", mode)
		}
	}
	_ = os.Chmod(path, 0600)
	_ = os.WriteFile(path, []byte(strings.Repeat("x", 513)), 0600)
	if _, e = r.Observe(context.Background(), "A"); e == nil {
		t.Fatal("read bound")
	}
	_ = os.Remove(path)
	_ = os.Symlink("missing", path)
	if _, e = r.Observe(context.Background(), "A"); e == nil {
		t.Fatal("symlink")
	}
	cfg.Path = "relative"
	if _, e = NewDynSecSnapshotReader(cfg); e == nil {
		t.Fatal("relative path")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e = r.Observe(ctx, "A"); e == nil {
		t.Fatal("cancelled snapshot")
	}
	parent := filepath.Join(dir, "alias")
	if e = os.Symlink(dir, parent); e != nil {
		t.Fatal(e)
	}
	if fd, e := openDynSecDirectory(parent); e == nil {
		_ = os.NewFile(uintptr(fd), "unexpected").Close()
		t.Fatal("parent symlink")
	}
}

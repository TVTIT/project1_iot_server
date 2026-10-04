package mqttcredential

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// Exact protected, schema-validated absence; never an unreadable file error.
var errDynSecSnapshotAbsent = errors.New("dynsec snapshot target absent")

// DynSecSnapshotConfig is trusted local configuration, never a request path.
// WriterUID must be the actual broker native writer UID (not reader's UID).
type DynSecSnapshotConfig struct {
	Path           string
	WriterUID      uint32
	MaxBytes       int64
	ValidateTarget func(string) bool
}
type DynSecSnapshotReader struct{ cfg DynSecSnapshotConfig }

func NewDynSecSnapshotReader(cfg DynSecSnapshotConfig) (*DynSecSnapshotReader, error) {
	if !filepath.IsAbs(cfg.Path) || filepath.Clean(cfg.Path) != cfg.Path || cfg.MaxBytes <= 0 || cfg.MaxBytes > 64<<20 || cfg.ValidateTarget == nil {
		return nil, dynSecError("snapshot_configuration")
	}
	return &DynSecSnapshotReader{cfg}, nil
}

// Observe returns selected native metadata only. It is NOT a live broker read,
// persistence/fsync receipt, complete-generation proof or verified success.
// An absent/unreadable/raced store is uncertain (error), never enabled=false.
// Walk directories with openat/O_NOFOLLOW so parent symlinks cannot redirect a
// trusted configured path. The trusted private mount itself is operator-owned.
func (r *DynSecSnapshotReader) Observe(ctx context.Context, target string) (DynSecClientMetadata, error) {
	bad := func() (DynSecClientMetadata, error) { return DynSecClientMetadata{}, dynSecError("snapshot_uncertain") }
	if ctx.Err() != nil || !r.cfg.ValidateTarget(target) {
		return bad()
	}
	dir, e := openDynSecDirectory(filepath.Dir(r.cfg.Path))
	if e != nil {
		return bad()
	}
	defer syscall.Close(dir)
	var ds syscall.Stat_t
	if syscall.Fstat(dir, &ds) != nil || ds.Mode&0077 != 0 || ds.Uid != r.cfg.WriterUID {
		return bad()
	}
	name := filepath.Base(r.cfg.Path)
	fd, e := syscall.Openat(dir, name, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if e != nil {
		return bad()
	}
	f := os.NewFile(uintptr(fd), "dynsec-snapshot")
	defer f.Close()
	before, e := f.Stat()
	if e != nil || !before.Mode().IsRegular() || before.Size() > r.cfg.MaxBytes || before.Mode().Perm()&0077 != 0 || before.Mode().Perm()&0400 == 0 {
		return bad()
	}
	stat, ok := before.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != r.cfg.WriterUID || stat.Nlink != 1 {
		return bad()
	}
	b, e := io.ReadAll(io.LimitReader(f, r.cfg.MaxBytes+1))
	if e != nil || int64(len(b)) > r.cfg.MaxBytes || ctx.Err() != nil {
		return bad()
	}
	defer clear(b)
	// Atomic native rename during read is retriable uncertainty, not a clear state.
	after, e := f.Stat()
	if e != nil || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return bad()
	}
	current, e := syscall.Openat(dir, name, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if e != nil {
		return bad()
	}
	other := os.NewFile(uintptr(current), "dynsec-snapshot-check")
	defer other.Close()
	now, e := other.Stat()
	if e != nil || !os.SameFile(before, now) || before.Size() != now.Size() || !before.ModTime().Equal(now.ModTime()) {
		return bad()
	}
	value, e := parseDynSecSnapshot(b, target)
	if errors.Is(e, errDynSecSnapshotAbsent) {
		return DynSecClientMetadata{}, e
	}
	if e != nil {
		return bad()
	}
	return value, nil
}

func openDynSecDirectory(path string) (int, error) {
	fd, e := syscall.Open("/", syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if e != nil {
		return -1, e
	}
	// Each path prefix is opened relative to an already-held descriptor.
	var parts []string
	for path != "/" {
		parts = append(parts, filepath.Base(path))
		path = filepath.Dir(path)
	}
	for i := len(parts) - 1; i >= 0; i-- {
		next, e := syscall.Openat(fd, parts[i], syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
		syscall.Close(fd)
		if e != nil {
			return -1, e
		}
		fd = next
	}
	return fd, nil
}

func parseDynSecSnapshot(b []byte, target string) (DynSecClientMetadata, error) {
	if strictDynSecJSON(b) != nil {
		return DynSecClientMetadata{}, dynSecError("snapshot_invalid")
	}
	var root map[string]json.RawMessage
	if json.Unmarshal(b, &root) != nil || root == nil {
		return DynSecClientMetadata{}, dynSecError("snapshot_invalid")
	}
	var clients []json.RawMessage
	if json.Unmarshal(root["clients"], &clients) != nil || clients == nil {
		return DynSecClientMetadata{}, dynSecError("snapshot_invalid")
	}
	seen := map[string]bool{}
	var found *DynSecClientMetadata
	for _, raw := range clients {
		var name struct {
			Username string `json:"username"`
		}
		if json.Unmarshal(raw, &name) != nil || name.Username == "" || seen[name.Username] {
			return DynSecClientMetadata{}, dynSecError("snapshot_invalid")
		}
		seen[name.Username] = true
		v, e := projectDynSecClient(raw, name.Username)
		if e != nil {
			return DynSecClientMetadata{}, dynSecError("snapshot_invalid")
		}
		if v.Username == target {
			found = &v
		}
	}
	if found == nil {
		return DynSecClientMetadata{}, errDynSecSnapshotAbsent
	}
	return *found, nil
}

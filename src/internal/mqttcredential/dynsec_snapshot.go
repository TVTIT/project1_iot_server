package mqttcredential

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
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

// DynSecSnapshotReader reads filesystem snapshots of dynamic security state.
type DynSecSnapshotReader struct{ cfg DynSecSnapshotConfig }

// NewDynSecSnapshotReader validates configuration and returns a DynSecSnapshotReader.
func NewDynSecSnapshotReader(cfg DynSecSnapshotConfig) (*DynSecSnapshotReader, error) {
	if !filepath.IsAbs(cfg.Path) || filepath.Clean(cfg.Path) != cfg.Path || cfg.MaxBytes <= 0 || cfg.MaxBytes > 64<<20 || cfg.ValidateTarget == nil {
		return nil, dynSecError("snapshot_configuration")
	}
	return &DynSecSnapshotReader{cfg}, nil
}

// readSnapshot performs bounded protected openat/O_NOFOLLOW, UID/mode/nlink
// identity checks and post-read race verification on the native store file.
// Byte buffer is cleared on return.
func (r *DynSecSnapshotReader) readSnapshot(ctx context.Context, parse func([]byte) error) error {
	bad := dynSecError("snapshot_uncertain")
	if ctx.Err() != nil {
		return bad
	}
	dir, e := openDynSecDirectory(filepath.Dir(r.cfg.Path))
	if e != nil {
		return bad
	}
	// Read-only descriptor cleanup cannot change the validated observation.
	defer func() { _ = syscall.Close(dir) }()
	var ds syscall.Stat_t
	if syscall.Fstat(dir, &ds) != nil || ds.Mode&0077 != 0 || ds.Uid != r.cfg.WriterUID {
		return bad
	}
	name := filepath.Base(r.cfg.Path)
	fd, e := syscall.Openat(dir, name, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if e != nil {
		return bad
	}
	f := os.NewFile(uintptr(fd), "dynsec-snapshot")
	defer func() { _ = f.Close() }() // Read-only cleanup; preserve the observation error.
	before, e := f.Stat()
	if e != nil || !before.Mode().IsRegular() || before.Size() > r.cfg.MaxBytes || before.Mode().Perm()&0077 != 0 || before.Mode().Perm()&0400 == 0 {
		return bad
	}
	stat, ok := before.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != r.cfg.WriterUID || stat.Nlink != 1 {
		return bad
	}
	b, e := io.ReadAll(io.LimitReader(f, r.cfg.MaxBytes+1))
	if e != nil || int64(len(b)) > r.cfg.MaxBytes || ctx.Err() != nil {
		return bad
	}
	defer clear(b)
	// Atomic native rename during read is retriable uncertainty, not a clear state.
	after, e := f.Stat()
	if e != nil || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return bad
	}
	current, e := syscall.Openat(dir, name, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if e != nil {
		return bad
	}
	other := os.NewFile(uintptr(current), "dynsec-snapshot-check")
	defer func() { _ = other.Close() }() // Read-only identity-check descriptor.
	now, e := other.Stat()
	if e != nil || !os.SameFile(before, now) || before.Size() != now.Size() || !before.ModTime().Equal(now.ModTime()) {
		return bad
	}
	return parse(b)
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
	var value DynSecClientMetadata
	e := r.readSnapshot(ctx, func(b []byte) error {
		v, parseErr := parseDynSecSnapshot(b, target)
		if parseErr != nil {
			return parseErr
		}
		value = v
		return nil
	})
	if errors.Is(e, errDynSecSnapshotAbsent) {
		return DynSecClientMetadata{}, e
	}
	if e != nil {
		return bad()
	}
	return value, nil
}

// VerifyRevoked reads one protected native snapshot observation and validates that:
//   - target exists, same username, disabled=true;
//   - exactly expected role "gateway_"+target with expected priority, no unexpected groups or client overrides;
//   - expected role definition exists exactly once with exact adapterGatewayACLs(target),
//     order-independent and canonical, rejecting duplicate ACLs and malformed types.
//
// A match is a filesystem observation only, NOT fsync or exact new-password persistence.
func (r *DynSecSnapshotReader) VerifyRevoked(ctx context.Context, target string) error {
	if ctx.Err() != nil || !r.cfg.ValidateTarget(target) {
		return dynSecError("snapshot_uncertain")
	}
	return r.readSnapshot(ctx, func(b []byte) error {
		return parseAndVerifyRevokedSnapshot(b, target)
	})
}

func parseAndVerifyRevokedSnapshot(b []byte, target string) error {
	if strictDynSecJSON(b) != nil {
		return dynSecError("snapshot_invalid")
	}
	var root map[string]json.RawMessage
	if json.Unmarshal(b, &root) != nil || root == nil {
		return dynSecError("snapshot_invalid")
	}

	// 1. Clients validation
	var clients []json.RawMessage
	if json.Unmarshal(root["clients"], &clients) != nil || clients == nil {
		return dynSecError("snapshot_invalid")
	}
	seenClients := map[string]bool{}
	var targetClientRaw json.RawMessage
	for _, raw := range clients {
		var name struct {
			Username string `json:"username"`
		}
		if json.Unmarshal(raw, &name) != nil || name.Username == "" || seenClients[name.Username] {
			return dynSecError("snapshot_invalid")
		}
		seenClients[name.Username] = true
		if name.Username == target {
			targetClientRaw = raw
		}
	}
	if targetClientRaw == nil {
		return dynSecError("snapshot_invalid")
	}
	meta, err := projectDynSecClient(targetClientRaw, target)
	if err != nil {
		return dynSecError("snapshot_invalid")
	}
	expectedRole := "gateway_" + target
	if !adapterClientMatches(meta, target, expectedRole, true) {
		return dynSecError("snapshot_invalid")
	}

	// 2. Roles validation
	var roles []json.RawMessage
	if json.Unmarshal(root["roles"], &roles) != nil || roles == nil {
		return dynSecError("snapshot_invalid")
	}
	seenRoles := map[string]bool{}
	var targetRoleRaw json.RawMessage
	for _, raw := range roles {
		var rName struct {
			Rolename string `json:"rolename"`
		}
		if json.Unmarshal(raw, &rName) != nil || rName.Rolename == "" || seenRoles[rName.Rolename] {
			return dynSecError("snapshot_invalid")
		}
		seenRoles[rName.Rolename] = true
		if rName.Rolename == expectedRole {
			targetRoleRaw = raw
		}
	}
	if targetRoleRaw == nil {
		return dynSecError("snapshot_invalid")
	}

	var roleFields map[string]json.RawMessage
	if json.Unmarshal(targetRoleRaw, &roleFields) != nil || roleFields == nil {
		return dynSecError("snapshot_invalid")
	}
	rawACLs, ok := roleFields["acls"]
	if !ok || string(rawACLs) == "null" {
		return dynSecError("snapshot_invalid")
	}
	var aclItems []json.RawMessage
	if json.Unmarshal(rawACLs, &aclItems) != nil || aclItems == nil {
		return dynSecError("snapshot_invalid")
	}

	wanted := adapterGatewayACLs(target)
	if len(aclItems) != len(wanted) {
		return dynSecError("snapshot_invalid")
	}

	var gotACLs []adapterACL
	for _, rawACL := range aclItems {
		var itemMap map[string]json.RawMessage
		if json.Unmarshal(rawACL, &itemMap) != nil || itemMap == nil {
			return dynSecError("snapshot_invalid")
		}
		rawType, okType := itemMap["acltype"]
		rawTopic, okTopic := itemMap["topic"]
		rawPriority, okPriority := itemMap["priority"]
		rawAllow, okAllow := itemMap["allow"]
		if !okType || !okTopic || !okPriority || !okAllow {
			return dynSecError("snapshot_invalid")
		}
		if string(rawType) == "null" || string(rawTopic) == "null" || string(rawPriority) == "null" || string(rawAllow) == "null" {
			return dynSecError("snapshot_invalid")
		}

		var aclType, topic string
		var priority int
		var allow bool
		if json.Unmarshal(rawType, &aclType) != nil || aclType == "" {
			return dynSecError("snapshot_invalid")
		}
		if json.Unmarshal(rawTopic, &topic) != nil || topic == "" {
			return dynSecError("snapshot_invalid")
		}

		var priNum json.Number
		dPri := json.NewDecoder(bytes.NewReader(rawPriority))
		dPri.UseNumber()
		if dPri.Decode(&priNum) != nil || dPri.More() {
			return dynSecError("snapshot_invalid")
		}
		if _, err := priNum.Int64(); err != nil {
			return dynSecError("snapshot_invalid")
		}
		if json.Unmarshal(rawPriority, &priority) != nil {
			return dynSecError("snapshot_invalid")
		}

		if json.Unmarshal(rawAllow, &allow) != nil {
			return dynSecError("snapshot_invalid")
		}

		gotACLs = append(gotACLs, adapterACL{
			Type:     aclType,
			Topic:    topic,
			Priority: priority,
			Allow:    allow,
		})
	}

	// Reject duplicate ACLs
	for i := 0; i < len(gotACLs); i++ {
		for j := i + 1; j < len(gotACLs); j++ {
			if reflect.DeepEqual(gotACLs[i], gotACLs[j]) {
				return dynSecError("snapshot_invalid")
			}
		}
	}

	// Canonical order-independent set equality against wanted ACLs
	for _, w := range wanted {
		count := 0
		for _, g := range gotACLs {
			if reflect.DeepEqual(w, g) {
				count++
			}
		}
		if count != 1 {
			return dynSecError("snapshot_invalid")
		}
	}

	return nil
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
		_ = syscall.Close(fd) // Release the previous read-only directory, preserving Openat's error.
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

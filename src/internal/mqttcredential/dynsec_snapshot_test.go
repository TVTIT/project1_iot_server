package mqttcredential

import (
	"context"
	"encoding/json"
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

func testBuildDynSecStore(target string, disabled bool, roleName string, acls []adapterACL, extraFields map[string]any) string {
	client := map[string]any{
		"username": target,
		"disabled": disabled,
	}
	if roleName != "" {
		client["roles"] = []any{map[string]any{"rolename": roleName, "priority": 0}}
	}
	for k, v := range extraFields {
		client[k] = v
	}
	roles := []any{}
	if roleName != "" && acls != nil {
		roles = append(roles, map[string]any{
			"rolename": roleName,
			"acls":     acls,
		})
	}
	payload := map[string]any{
		"clients": []any{client},
		"roles":   roles,
	}
	b, _ := json.Marshal(payload)
	return string(b)
}

func TestDynSecVerifyRevoked_Success(t *testing.T) {
	dir := t.TempDir()
	_ = os.Chmod(dir, 0700)
	path := filepath.Join(dir, "native.json")
	cfg := DynSecSnapshotConfig{Path: path, WriterUID: uint32(os.Getuid()), MaxBytes: 65536, ValidateTarget: func(s string) bool { return s == "A" }}
	r, err := NewDynSecSnapshotReader(cfg)
	if err != nil {
		t.Fatal(err)
	}

	acls := adapterGatewayACLs("A")
	store := testBuildDynSecStore("A", true, "gateway_A", acls, nil)
	if err := os.WriteFile(path, []byte(store), 0600); err != nil {
		t.Fatal(err)
	}

	if err := r.VerifyRevoked(context.Background(), "A"); err != nil {
		t.Fatalf("expected verify revoked success, got: %v", err)
	}

	// Shuffled ACL order must succeed canonically
	shuffled := make([]adapterACL, len(acls))
	copy(shuffled, acls)
	shuffled[0], shuffled[len(shuffled)-1] = shuffled[len(shuffled)-1], shuffled[0]
	shuffledStore := testBuildDynSecStore("A", true, "gateway_A", shuffled, nil)
	if err := os.WriteFile(path, []byte(shuffledStore), 0600); err != nil {
		t.Fatal(err)
	}
	if err := r.VerifyRevoked(context.Background(), "A"); err != nil {
		t.Fatalf("expected verify revoked success with shuffled ACLs, got: %v", err)
	}
}

func TestDynSecVerifyRevoked_Mismatch(t *testing.T) {
	dir := t.TempDir()
	_ = os.Chmod(dir, 0700)
	path := filepath.Join(dir, "native.json")
	cfg := DynSecSnapshotConfig{Path: path, WriterUID: uint32(os.Getuid()), MaxBytes: 65536, ValidateTarget: func(s string) bool { return s == "A" }}
	r, err := NewDynSecSnapshotReader(cfg)
	if err != nil {
		t.Fatal(err)
	}

	baseACLs := adapterGatewayACLs("A")

	// 1. enabled target
	cases := []struct {
		name string
		raw  string
	}{
		{"enabled_target", testBuildDynSecStore("A", false, "gateway_A", baseACLs, nil)},
		{"missing_target", testBuildDynSecStore("B", true, "gateway_A", baseACLs, nil)},
		{"wrong_role", testBuildDynSecStore("A", true, "wrong_role", baseACLs, nil)},
		{"unexpected_group", testBuildDynSecStore("A", true, "gateway_A", baseACLs, map[string]any{"groups": []any{map[string]any{"groupname": "admin"}}})},
		{"client_id_override", testBuildDynSecStore("A", true, "gateway_A", baseACLs, map[string]any{"clientid": "override"})},
		{"missing_acl", testBuildDynSecStore("A", true, "gateway_A", baseACLs[:len(baseACLs)-1], nil)},
		{"extra_acl", testBuildDynSecStore("A", true, "gateway_A", append(baseACLs, adapterACL{Type: "publishClientSend", Topic: "#", Allow: true, Priority: 0}), nil)},
		{"wrong_gateway_acl", testBuildDynSecStore("A", true, "gateway_A", adapterGatewayACLs("B"), nil)},
		{"wrong_allow_acl", func() string {
			bad := make([]adapterACL, len(baseACLs))
			copy(bad, baseACLs)
			bad[0].Allow = false
			return testBuildDynSecStore("A", true, "gateway_A", bad, nil)
		}()},
		{"wrong_priority_acl", func() string {
			bad := make([]adapterACL, len(baseACLs))
			copy(bad, baseACLs)
			bad[0].Priority = 1
			return testBuildDynSecStore("A", true, "gateway_A", bad, nil)
		}()},
		{"duplicate_acl", func() string {
			dups := append(baseACLs, baseACLs[0])
			return testBuildDynSecStore("A", true, "gateway_A", dups, nil)
		}()},
		{"duplicate_expected_role", `{"clients":[{"username":"A","disabled":true,"roles":[{"rolename":"gateway_A"}]}],"roles":[{"rolename":"gateway_A","acls":[]},{"rolename":"gateway_A","acls":[]}]}`},
		{"missing_role_definition", `{"clients":[{"username":"A","disabled":true,"roles":[{"rolename":"gateway_A"}]}],"roles":[]}`},
		{"duplicate_client_username", `{"clients":[{"username":"A","disabled":true},{"username":"A","disabled":true}],"roles":[]}`},
		{"duplicate_json_keys", `{"clients":[{"username":"A","disabled":true,"disabled":true,"roles":[{"rolename":"gateway_A"}]}],"roles":[]}`},
		{"missing_allow_in_acl", `{"clients":[{"username":"A","disabled":true,"roles":[{"rolename":"gateway_A"}]}],"roles":[{"rolename":"gateway_A","acls":[{"acltype":"publishClientSend","topic":"gateways/A/telemetry/#","priority":0}]}]}`},
		{"null_allow_in_acl", `{"clients":[{"username":"A","disabled":true,"roles":[{"rolename":"gateway_A"}]}],"roles":[{"rolename":"gateway_A","acls":[{"acltype":"publishClientSend","topic":"gateways/A/telemetry/#","priority":0,"allow":null}]}]}`},
		{"string_allow_in_acl", `{"clients":[{"username":"A","disabled":true,"roles":[{"rolename":"gateway_A"}]}],"roles":[{"rolename":"gateway_A","acls":[{"acltype":"publishClientSend","topic":"gateways/A/telemetry/#","priority":0,"allow":"true"}]}]}`},
		{"missing_priority_in_acl", `{"clients":[{"username":"A","disabled":true,"roles":[{"rolename":"gateway_A"}]}],"roles":[{"rolename":"gateway_A","acls":[{"acltype":"publishClientSend","topic":"gateways/A/telemetry/#","allow":true}]}]}`},
		{"null_priority_in_acl", `{"clients":[{"username":"A","disabled":true,"roles":[{"rolename":"gateway_A"}]}],"roles":[{"rolename":"gateway_A","acls":[{"acltype":"publishClientSend","topic":"gateways/A/telemetry/#","priority":null,"allow":true}]}]}`},
		{"string_priority_in_acl", `{"clients":[{"username":"A","disabled":true,"roles":[{"rolename":"gateway_A"}]}],"roles":[{"rolename":"gateway_A","acls":[{"acltype":"publishClientSend","topic":"gateways/A/telemetry/#","priority":"0","allow":true}]}]}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(tc.raw), 0600); err != nil {
				t.Fatal(err)
			}
			if err := r.VerifyRevoked(context.Background(), "A"); err == nil {
				t.Fatalf("expected verification failure for %s, got nil", tc.name)
			}
		})
	}
}

func TestDynSecVerifyRevoked_ProtectedAndNoSecretLeaks(t *testing.T) {
	dir := t.TempDir()
	_ = os.Chmod(dir, 0700)
	path := filepath.Join(dir, "native.json")
	cfg := DynSecSnapshotConfig{Path: path, WriterUID: uint32(os.Getuid()), MaxBytes: 4096, ValidateTarget: func(s string) bool { return s == "A" }}
	r, err := NewDynSecSnapshotReader(cfg)
	if err != nil {
		t.Fatal(err)
	}

	secretSentinel := "TOP_SECRET_PASSWORD_12345"
	saltSentinel := "SECRET_SALT_VALUE_67890"
	hashSentinel := "SECRET_HASH_VALUE_ABCDE"

	storeWithSecrets := `{
		"clients": [{
			"username": "A",
			"disabled": true,
			"password": "` + secretSentinel + `",
			"salt": "` + saltSentinel + `",
			"hash": "` + hashSentinel + `",
			"roles": [{"rolename":"gateway_A","priority":0}]
		}],
		"roles": [{
			"rolename": "gateway_A",
			"acls": ` + func() string {
		b, _ := json.Marshal(adapterGatewayACLs("A"))
		return string(b)
	}() + `
		}]
	}`

	if err := os.WriteFile(path, []byte(storeWithSecrets), 0600); err != nil {
		t.Fatal(err)
	}

	// 1. Success on valid store with secret fields present, without leaking them
	if err := r.VerifyRevoked(context.Background(), "A"); err != nil {
		t.Fatalf("expected success on valid store with unmapped secret fields: %v", err)
	}

	// 2. Secret sentinel never appears in error strings
	invalidWithSecrets := `{
		"clients": [{
			"username": "A",
			"disabled": false,
			"password": "` + secretSentinel + `",
			"salt": "` + saltSentinel + `",
			"hash": "` + hashSentinel + `",
			"roles": [{"rolename":"gateway_A"}]
		}],
		"roles": []
	}`
	if err := os.WriteFile(path, []byte(invalidWithSecrets), 0600); err != nil {
		t.Fatal(err)
	}
	err = r.VerifyRevoked(context.Background(), "A")
	if err == nil {
		t.Fatal("expected error for enabled target")
	}
	errStr := err.Error()
	for _, sentinel := range []string{secretSentinel, saltSentinel, hashSentinel} {
		if strings.Contains(errStr, sentinel) {
			t.Fatalf("secret sentinel leaked in error string: %s", sentinel)
		}
	}

	// 3. Protected I/O checks: bad mode, symlink, oversize, cancelled context
	// Bad mode
	_ = os.Chmod(path, 0644)
	if err := r.VerifyRevoked(context.Background(), "A"); err == nil {
		t.Fatal("expected error on mode 0644")
	}
	_ = os.Chmod(path, 0600)

	// Oversize
	_ = os.WriteFile(path, []byte(strings.Repeat(" ", 4097)), 0600)
	if err := r.VerifyRevoked(context.Background(), "A"); err == nil {
		t.Fatal("expected error on oversize file")
	}

	// Symlink
	_ = os.Remove(path)
	_ = os.Symlink("missing", path)
	if err := r.VerifyRevoked(context.Background(), "A"); err == nil {
		t.Fatal("expected error on symlink")
	}

	// Cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.VerifyRevoked(ctx, "A"); err == nil {
		t.Fatal("expected error on cancelled context")
	}
}

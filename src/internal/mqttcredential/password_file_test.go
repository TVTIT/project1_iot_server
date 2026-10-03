//nolint:errcheck,misspell // Test descriptor cleanup; mosquitto_passwd is the native product name.
package mqttcredential

import (
	"crypto/sha512"
	"encoding/base64"
	"os"
	"strings"
	"testing"

	"golang.org/x/crypto/pbkdf2"
)

// Test fixture only; production hash generation belongs to mosquitto_passwd.
func testHash(password string) string {
	salt := []byte("test-salt-12")
	return "$7$101$" + base64.StdEncoding.EncodeToString(salt) + "$" + base64.StdEncoding.EncodeToString(pbkdf2.Key([]byte(password), salt, 101, 64, sha512.New))
}
func testFile() string {
	return BackendUsername + ":" + testHash("backend-test") + "\ngw_test:" + testHash("old-test") + "\n"
}

func TestParser(t *testing.T) {
	good := testFile()
	if _, err := ParsePasswordFile(strings.NewReader(good), DefaultMaxFileBytes); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", good + "\n", good + "gw_test:" + testHash("x") + "\n", strings.Replace(good, "$7$", "$6$", 1), strings.Replace(good, "$101$", "$102$", 1), strings.Replace(good, "$101$", "$0101$", 1), "gw_test:" + testHash("x") + "\n", strings.Replace(good, "gw_test:", "bad/name:", 1), strings.Replace(good, "gw_test:", " gw_test:", 1), strings.Replace(good, "gw_test:", "gw\x00_test:", 1), good + "bad:plaintext\n", strings.Replace(good, "\n", "\r\n", 1), BackendUsername + ":$7$101$YWJj$YWJj\n"} {
		if _, err := ParsePasswordFile(strings.NewReader(bad), DefaultMaxFileBytes); err == nil {
			t.Errorf("accepted malformed fixture")
		}
	}
	if _, err := ParsePasswordFile(strings.NewReader(good), int64(len(good)-1)); err == nil {
		t.Fatal("accepted oversized file")
	}
	if _, err := ParsePasswordFile(strings.NewReader(good), int64(len(good))); err != nil {
		t.Fatal(err)
	}
}
func TestVerifyHash(t *testing.T) {
	h := testHash("  unchanged password  ")
	for _, p := range []string{"  unchanged password  ", "unchanged password"} {
		ok, err := VerifyPasswordHash(h, p)
		if err != nil || ok != (p == "  unchanged password  ") {
			t.Fatal("verification mismatch")
		}
	}
	for _, p := range []string{"", "x\x00", "x\n", "x\r"} {
		if _, err := VerifyPasswordHash(h, p); err == nil {
			t.Fatal("invalid password accepted")
		}
	}
}

// Opt-in disposable native fixture, generated with the pinned PoC image.
func TestPinnedNativeCompatibility(t *testing.T) {
	path := os.Getenv("MQTT_TEST_NATIVE_FIXTURE")
	if path == "" {
		t.Skip("no disposable pinned native fixture supplied")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	entries, err := ParsePasswordFile(f, DefaultMaxFileBytes)
	if err != nil {
		t.Fatal(err)
	}
	for _, hash := range entries {
		ok, err := VerifyPasswordHash(hash, "disposable-native-test")
		if err != nil || !ok {
			t.Fatal("native password verification mismatch")
		}
		ok, err = VerifyPasswordHash(hash, "wrong-test")
		if err != nil || ok {
			t.Fatal("native wrong password accepted")
		}
	}
}

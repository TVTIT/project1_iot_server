package mqttcredential

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/google/uuid"
)

type secretReaderFunc func([]byte) (int, error)

func (f secretReaderFunc) Read(p []byte) (int, error) { return f(p) }

func TestSecretSelectedReaderAndEncoding(t *testing.T) {
	for _, value := range []byte{0, 0xff, 0xfb} {
		source := bytes.Repeat([]byte{value}, 33)
		reader := bytes.NewReader(source)
		password, err := generatePasswordFrom(reader)
		if err != nil {
			t.Fatal("selected reader failed")
		}
		if password != base64.RawURLEncoding.EncodeToString(source[:32]) || reader.Len() != 1 {
			t.Fatal("did not consume exactly the selected 32 bytes")
		}
		assertSecretEncoding(t, password)
	}
}

func assertSecretEncoding(t *testing.T, password string) {
	t.Helper()
	if len(password) != 43 || strings.ContainsAny(password, "=+/") {
		t.Fatal("incorrect raw Base64URL length or padding")
	}
	for _, c := range password {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' && c != '_' {
			t.Fatal("unexpected password alphabet")
		}
	}
	decoded, err := base64.RawURLEncoding.DecodeString(password)
	if err != nil || len(decoded) != 32 {
		t.Fatal("password does not encode 32 bytes")
	}
}

func TestSecretPartialReadsAndBufferClear(t *testing.T) {
	source := bytes.Repeat([]byte{0xfb}, 32)
	offset, calls := 0, 0
	var retained []byte
	reader := secretReaderFunc(func(p []byte) (int, error) {
		if retained == nil {
			retained = p
		}
		calls++
		n := copy(p[:min(3, len(p))], source[offset:])
		offset += n
		return n, nil
	})
	password, err := generatePasswordFrom(reader)
	if err != nil || password != base64.RawURLEncoding.EncodeToString(source) || offset != 32 || calls != 11 {
		t.Fatal("partial reads did not produce the full selected input")
	}
	if !bytes.Equal(retained, make([]byte, 32)) {
		t.Fatal("temporary byte buffer was not cleared")
	}
}

func TestSecretFailuresAreSafeAndNoFallback(t *testing.T) {
	const diagnostic = "sensitive-reader-diagnostic"
	for _, tc := range []struct {
		name   string
		reader io.Reader
	}{
		{"nil", nil},
		{"empty", bytes.NewReader(nil)},
		{"short", bytes.NewReader(make([]byte, 31))},
		{"error", secretReaderFunc(func([]byte) (int, error) { return 0, errors.New(diagnostic) })},
		{"partial_error", secretReaderFunc(func(p []byte) (int, error) { p[0] = 0xff; return 1, errors.New(diagnostic) })},
	} {
		t.Run(tc.name, func(t *testing.T) {
			password, err := generatePasswordFrom(tc.reader)
			var domain *DomainError
			if password != "" || !errors.As(err, &domain) || domain.Code != CodeInternalError {
				t.Fatal("generation failure did not fail closed with safe classification")
			}
			if err.Error() != string(CodeInternalError) || errors.Unwrap(err) != nil || strings.Contains(fmt.Sprintf("%+v %#v", err, err), diagnostic) {
				t.Fatal("reader diagnostic escaped into domain error")
			}
		})
	}
	var retained []byte
	_, err := generatePasswordFrom(secretReaderFunc(func(p []byte) (int, error) {
		retained = p
		p[0] = 0xff
		return 1, errors.New(diagnostic)
	}))
	if err == nil || !bytes.Equal(retained, make([]byte, 32)) {
		t.Fatal("failure did not clear temporary byte buffer")
	}
}

func TestSecretNoProgressFailsWithoutRetry(t *testing.T) {
	calls := 0
	password, err := generatePasswordFrom(secretReaderFunc(func([]byte) (int, error) {
		calls++
		if calls > 1 {
			return 0, io.EOF // Keeps a broken implementation from hanging the test.
		}
		return 0, nil
	}))
	if password != "" || err == nil || calls != 1 {
		t.Fatal("no-progress reader was retried")
	}
}

func TestSecretRandomSources(t *testing.T) {
	// Shape/source plumbing only: no histogram or probabilistic entropy claim.
	for _, generate := range []func() (string, error){generatePassword, func() (string, error) { return generatePasswordFrom(rand.Reader) }} {
		password, err := generate()
		if err != nil {
			t.Fatal("CSPRNG generation failed")
		}
		assertSecretEncoding(t, password)
	}
}

func TestSecretResultLifecycle(t *testing.T) {
	password, err := generatePasswordFrom(bytes.NewReader(bytes.Repeat([]byte{0xfb}, 32)))
	if err != nil {
		t.Fatal("fixture generation failed")
	}
	result := NewSecretResult(Metadata{}, uuid.Nil, password)
	for _, value := range []any{result, &result, result.Metadata, MutationResult{}} {
		if strings.Contains(fmt.Sprintf("%v %+v %#v %s", value, value, value, value), password) {
			t.Fatal("formatter disclosed generated secret")
		}
	}
	meta, err := json.Marshal(result.Metadata)
	if err != nil || strings.Contains(string(meta), password) {
		t.Fatal("metadata disclosed generated secret")
	}
	first, err := json.Marshal(result)
	if err != nil || !strings.Contains(string(first), password) || !strings.Contains(string(first), `"secret_returned":true`) {
		t.Fatal("explicit first response missing secret")
	}
	result.ClearSecret()
	result.ClearSecret()
	cleared, err := json.Marshal(result)
	if err != nil || strings.Contains(string(cleared), password) || strings.Contains(string(cleared), `"password"`) || !strings.Contains(string(cleared), `"secret_returned":false`) {
		t.Fatal("cleared result still emits secret")
	}
}

package mqttcredential

import (
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"io"
	"strings"

	"golang.org/x/crypto/pbkdf2"

	"iot-platform/internal/gateway"
)

// BackendUsername is the protected internal broker principal.
const BackendUsername = "backend_service"

// DefaultMaxFileBytes bounds password-file parsing by default.
const DefaultMaxFileBytes int64 = 1 << 20

// ParsePasswordFile accepts only the native 2.0.18 sha512-pbkdf2 format.
// Iterations are deliberately restricted to the pinned utility's default (101).
// Legacy SHA512, plaintext, whitespace normalization and duplicate users fail closed.
func ParsePasswordFile(r io.Reader, maxBytes int64) (map[string]string, error) {
	if maxBytes <= 0 || maxBytes > 64<<20 {
		return nil, ErrInvalidInput
	}
	b, err := io.ReadAll(io.LimitReader(r, maxBytes+1))
	if err != nil {
		return nil, ErrPersistenceFailure
	}
	if int64(len(b)) > maxBytes || len(b) == 0 || strings.ContainsAny(string(b), "\x00\r") {
		return nil, ErrInvalidInput
	}
	entries := make(map[string]string)
	text := strings.TrimSuffix(string(b), "\n")
	for _, line := range strings.Split(text, "\n") {
		user, hash, ok := strings.Cut(line, ":")
		if !ok || (user != BackendUsername && gateway.ValidateGatewayID(user) != nil) {
			return nil, ErrInvalidInput
		}
		if _, exists := entries[user]; exists {
			return nil, ErrInvalidInput
		}
		if _, _, err := decodeHash(hash); err != nil {
			return nil, err
		}
		entries[user] = hash
	}
	if _, ok := entries[BackendUsername]; !ok {
		return nil, ErrInvalidInput
	}
	return entries, nil
}

func decodeHash(hash string) ([]byte, []byte, error) {
	p := strings.Split(hash, "$")
	if len(p) != 5 || p[0] != "" || p[1] != "7" || p[2] != "101" {
		return nil, nil, ErrInvalidInput
	}
	decode := func(s string, size int) ([]byte, error) {
		b, err := base64.StdEncoding.Strict().DecodeString(s)
		if err != nil || len(b) != size || base64.StdEncoding.EncodeToString(b) != s {
			return nil, ErrInvalidInput
		}
		return b, nil
	}
	salt, err := decode(p[3], 12)
	if err != nil {
		return nil, nil, err
	}
	digest, err := decode(p[4], 64)
	if err != nil {
		return nil, nil, err
	}
	return salt, digest, nil
}

// VerifyPasswordHash is for a future initializer, not a custom hash generator.
func VerifyPasswordHash(hash, password string) (bool, error) {
	if !validPassword(password) {
		return false, ErrInvalidInput
	}
	salt, expected, err := decodeHash(hash)
	if err != nil {
		return false, err
	}
	actual := pbkdf2.Key([]byte(password), salt, 101, 64, sha512.New)
	return subtle.ConstantTimeCompare(actual, expected) == 1, nil
}

func validPassword(p string) bool {
	return len(p) > 0 && len(p) <= 4096 && !strings.ContainsAny(p, "\r\n\x00")
}

func validateMutation(before, after map[string]string, target string, remove bool) error {
	for user, hash := range before {
		if user != target && after[user] != hash {
			return ErrToolFailure
		}
	}
	for user := range after {
		if user != target {
			if _, ok := before[user]; !ok {
				return ErrToolFailure
			}
		}
	}
	_, present := after[target]
	if present == remove {
		return ErrToolFailure
	}
	return nil
}

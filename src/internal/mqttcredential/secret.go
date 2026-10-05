package mqttcredential

import (
	"crypto/rand"
	"encoding/base64"
	"io"
)

// generatePassword is the production CSPRNG entry point. The service must call
// it only after authorized admission and confirmed DB intent, with Gateway
// routes closed and old sessions drained; never generate for queued requests,
// replays or revocations. Failure must stop before any broker mutation.
func generatePassword() (string, error) {
	return generatePasswordFrom(rand.Reader)
}

// generatePasswordFrom lets the service inject a per-instance source without
// mutating a global reader. Production sources must be cryptographically secure;
// deterministic readers are for tests only. A nil source fails closed rather
// than silently selecting a default. Callers own reader concurrency safety.
//
// The returned string is transient operation-local material: do not log, persist,
// queue, cache or put it in subprocess argv/environment. Package in SecretResult
// only after verified finalization, then ClearSecret and drop local references.
func generatePasswordFrom(source io.Reader) (string, error) {
	if source == nil {
		return "", &DomainError{Code: CodeInternalError}
	}
	var raw [32]byte
	// Best effort for this temporary buffer only. Go may retain other copies,
	// particularly the encoded immutable string; this is not heap zeroization.
	defer clear(raw[:])
	if _, err := io.ReadFull(secretProgressReader{source}, raw[:]); err != nil {
		// Never wrap a source error: it may contain sensitive diagnostics.
		return "", &DomainError{Code: CodeInternalError}
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

// secretProgressReader makes ReadFull fail immediately on (0, nil), rather than
// retrying indefinitely. Each successful read advances the fixed 32-byte buffer,
// so at most 32 reads are needed. A blocking Reader must be bounded by its owner.
type secretProgressReader struct {
	io.Reader
}

func (r secretProgressReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n == 0 && err == nil {
		return 0, io.ErrNoProgress
	}
	return n, err
}

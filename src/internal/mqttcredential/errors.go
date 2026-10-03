// Package mqttcredential manages a private native broker password file.
package mqttcredential

import "errors"

var (
	// ErrInvalidInput rejects identifiers, passwords or malformed runtime settings.
	ErrInvalidInput = errors.New("invalid credential input")
	// ErrRuntimeBusy reports bounded admission exhaustion.
	ErrRuntimeBusy = errors.New("credential runtime busy")
	// ErrToolFailure hides native utility diagnostics from callers.
	ErrToolFailure = errors.New("password utility failed")
	// ErrPersistenceFailure reports a filesystem operation failure.
	ErrPersistenceFailure = errors.New("credential persistence failed")
	// ErrReloadUnavailable reports unavailable signal delivery.
	ErrReloadUnavailable = errors.New("credential reload unavailable")
	// ErrVerificationFailed means fresh authentication did not verify the operation.
	ErrVerificationFailed = errors.New("credential verification failed")
	// ErrRecoveryRequired blocks mutation until an administrator resolves recovery.
	ErrRecoveryRequired = errors.New("credential recovery required")
)

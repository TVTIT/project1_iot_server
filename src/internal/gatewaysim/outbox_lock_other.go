//go:build !linux

package gatewaysim

import "fmt"

// lockOutbox is only implemented for the Linux simulator target. Other
// platforms fail closed rather than silently allowing concurrent writers.
func lockOutbox(path string) (func() error, error) {
	return nil, fmt.Errorf("%w: outbox file locking is only implemented on linux", ErrOutboxBusy)
}

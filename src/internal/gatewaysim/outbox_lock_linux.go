//go:build linux

package gatewaysim

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// lockOutbox takes an exclusive non-blocking flock on a stable sidecar file so
// only one simulator or admin process mutates one outbox at a time. It reuses
// the repository's existing unix flock pattern and is deliberate single-host
// admission, not a distributed fencing protocol. The sidecar inode is stable:
// it is created if missing and its lock is released by Close, never unlinked.
func lockOutbox(path string) (func() error, error) {
	f, err := os.OpenFile(path+".lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrOutboxBusy, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: lock already held", ErrOutboxBusy)
		}
		return nil, err
	}
	return func() error {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		return f.Close()
	}, nil
}

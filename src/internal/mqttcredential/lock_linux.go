//go:build linux

package mqttcredential

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// Stable slot files implement bounded admission across instances AND processes.
// All participants must use the same MaxPending. Never unlink lock/slot inodes.
func acquireLock(ctx context.Context, dir string, maxPending int) (func(), error) {
	var slot *os.File
	for i := 0; i < maxPending+1; i++ {
		f, err := openSecure(filepath.Join(dir, fmt.Sprintf(".credential.slot.%d", i)), syscall.O_RDWR|syscall.O_CREAT)
		if err != nil {
			return nil, err
		}
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			slot = f
			break
		}
		_ = f.Close()
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrPersistenceFailure
		}
	}
	if slot == nil {
		return nil, ErrRuntimeBusy
	}
	releaseSlot := func() { _ = syscall.Flock(int(slot.Fd()), syscall.LOCK_UN); _ = slot.Close() }
	f, err := openSecure(filepath.Join(dir, ".credential.lock"), syscall.O_RDWR|syscall.O_CREAT)
	if err != nil {
		releaseSlot()
		return nil, err
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			_ = f.Close()
			releaseSlot()
			return nil, ErrRuntimeBusy
		}
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close(); releaseSlot() }, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			_ = f.Close()
			releaseSlot()
			return nil, ErrPersistenceFailure
		}
		select {
		case <-ctx.Done():
			_ = f.Close()
			releaseSlot()
			return nil, ErrRuntimeBusy
		case <-ticker.C:
		}
	}
}

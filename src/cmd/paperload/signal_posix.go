//go:build !linux && (darwin || freebsd || openbsd || netbsd || dragonfly || solaris || illumos || aix)

package main

import (
	"errors"
	"os"
	"syscall"
)

func terminationSignal() os.Signal { return syscall.SIGTERM }

func notifyPlannedViewerStop() (<-chan struct{}, func(), error) {
	return nil, func() {}, errors.New("planned viewer stop is Linux-only")
}

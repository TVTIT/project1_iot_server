//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly && !solaris && !illumos && !aix

package main

import (
	"errors"
	"os"
)

func terminationSignal() os.Signal { return os.Interrupt }

func notifyPlannedViewerStop() (<-chan struct{}, func(), error) {
	return nil, func() {}, errors.New("planned viewer stop is Linux-only")
}

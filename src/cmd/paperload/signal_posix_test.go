//go:build !linux && (darwin || freebsd || openbsd || netbsd || dragonfly || solaris || illumos || aix)

package main

import (
	"syscall"
	"testing"
)

func TestDefaultTerminationSignalRemainsSIGTERM(t *testing.T) {
	if terminationSignal() != syscall.SIGTERM {
		t.Fatalf("default termination signal=%v, want SIGTERM", terminationSignal())
	}
}

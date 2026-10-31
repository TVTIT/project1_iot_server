//go:build linux

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

//go:build linux

package main

import (
	"os"
	"os/signal"
	"syscall"
)

func terminationSignal() os.Signal { return syscall.SIGTERM }

func notifyPlannedViewerStop() (<-chan struct{}, func(), error) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGUSR1)
	stop, cancel := signalStopChannel(signals)
	return stop, func() {
		signal.Stop(signals)
		cancel()
	}, nil
}

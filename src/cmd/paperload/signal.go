package main

import "os"

func signalStopChannel(signals <-chan os.Signal) (<-chan struct{}, func()) {
	stop := make(chan struct{})
	cancel := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-signals:
			close(stop)
		case <-cancel:
		}
	}()
	return stop, func() {
		close(cancel)
		<-done
	}
}

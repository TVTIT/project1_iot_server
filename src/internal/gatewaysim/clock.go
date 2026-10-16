package gatewaysim

import "time"

// The scheduler reads time and creates timers through this narrow boundary so a
// test can drive generation cadence, ACK timeouts and retry backoff
// deterministically without sleeping on wall-clock time. Only the scheduler uses
// it; the outbox and transport keep using real time.

// Timer is the minimal timer surface the scheduler uses. It mirrors
// *time.Timer so the production clock can wrap it directly.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

// Clock abstracts the monotonic source the scheduler schedules on. Now is used
// for measured_at, readiness cadence and retry deadlines; NewTimer arms the one
// scheduler wake timer.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
}

// realClock is the production clock backed by the standard library.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTimer(d time.Duration) Timer { return &realTimer{timer: time.NewTimer(d)} }

// realTimer adapts *time.Timer to Timer.
type realTimer struct{ timer *time.Timer }

func (r *realTimer) C() <-chan time.Time { return r.timer.C }

func (r *realTimer) Stop() bool { return r.timer.Stop() }

func (r *realTimer) Reset(d time.Duration) bool { return r.timer.Reset(d) }

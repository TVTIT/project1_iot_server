package mqttcredential

import "sync/atomic"

// StartupReadiness reports only the completed startup barrier. It is not a
// post-OPEN database-loss policy or a continuous broker/HA health assertion.
type StartupReadiness struct{ ready atomic.Bool }

// Ready returns true if the startup barrier has been completed.
func (r *StartupReadiness) Ready() bool { return r.ready.Load() }
func (r *StartupReadiness) set(v bool)  { r.ready.Store(v) }

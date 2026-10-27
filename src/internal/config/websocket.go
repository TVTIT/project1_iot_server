package config

import (
	"fmt"
	"time"
)

func loadWebSocket(lookup LookupFunc) (WebSocketConfig, error) {
	var c WebSocketConfig
	var err error
	ints := []struct {
		key      string
		dst      *int
		def, max int
	}{
		{"WS_MAX_SESSIONS", &c.MaxSessions, 32, 32},
		{"WS_MAX_SUBSCRIPTIONS_PER_SESSION", &c.MaxSubscriptionsPerSession, 16, 16},
		{"WS_GLOBAL_EVENT_SLOTS", &c.GlobalEventSlots, 64, 64},
		{"WS_GLOBAL_EVENT_BYTES", &c.GlobalEventBytes, 8 << 20, 8 << 20},
		{"WS_SESSION_EVENT_SLOTS", &c.SessionEventSlots, 64, 64},
		{"WS_SESSION_EVENT_BYTES", &c.SessionEventBytes, 256 << 10, 256 << 10},
		{"WS_MAX_FRAME_BYTES", &c.MaxFrameBytes, 66560, 66560},
		{"WS_READ_FRAME_BYTES", &c.ReadFrameBytes, 4096, 4096},
		{"WS_REQUEST_RATE_PER_SECOND", &c.RequestRatePerSecond, 10, 10},
		{"WS_RECHECK_CONCURRENCY", &c.RecheckConcurrency, 2, 2},
	}
	for _, v := range ints {
		if *v.dst, err = integer(lookup, v.key, v.def, 1, v.max); err != nil {
			return WebSocketConfig{}, err
		}
	}
	durations := []struct {
		key      string
		dst      *time.Duration
		def, max time.Duration
	}{
		{"WS_RECHECK_INTERVAL", &c.RecheckInterval, 5 * time.Second, time.Minute},
		{"WS_RECHECK_SWEEP_BUDGET", &c.RecheckSweepBudget, 2 * time.Second, 10 * time.Second},
		{"WS_HEARTBEAT_INTERVAL", &c.HeartbeatInterval, 20 * time.Second, 5 * time.Minute},
		{"WS_PONG_TIMEOUT", &c.PongTimeout, 45 * time.Second, 10 * time.Minute},
		{"WS_WRITE_TIMEOUT", &c.WriteTimeout, 5 * time.Second, time.Minute},
	}
	for _, v := range durations {
		if *v.dst, err = boundedDuration(lookup, v.key, v.def, time.Millisecond, v.max); err != nil {
			return WebSocketConfig{}, err
		}
	}
	if c.RecheckSweepBudget >= c.RecheckInterval {
		return WebSocketConfig{}, fmt.Errorf("WS_RECHECK_SWEEP_BUDGET must be less than WS_RECHECK_INTERVAL")
	}
	if c.PongTimeout <= c.HeartbeatInterval {
		return WebSocketConfig{}, fmt.Errorf("WS_PONG_TIMEOUT must exceed WS_HEARTBEAT_INTERVAL")
	}
	if c.WriteTimeout >= c.HeartbeatInterval {
		return WebSocketConfig{}, fmt.Errorf("WS_WRITE_TIMEOUT must be less than WS_HEARTBEAT_INTERVAL")
	}
	return c, nil
}
